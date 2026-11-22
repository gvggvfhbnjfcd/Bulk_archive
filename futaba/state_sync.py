"""
Keeps the "already archived" state across jobs.

A GitHub-hosted runner starts every job with an empty disk, so the list of archived threads lives in
a single-commit branch of this repo (STATE_BRANCH, default `futaba-state`, file state.db.gz) that is
force-pushed, so it never grows the repo's history.

Only threads whose bundle IA has ACCEPTED are saved there (apply_manifest runs after each upload).
Threads still sitting in an unsent bundle are in the job's working state.db only; if the job dies
before uploading them they are simply archived again by the next job.

  restore(data_dir)   job start: fetch the saved state into committed.db and state.db
                      (does nothing if state.db already exists, e.g. a restart inside the same job).
                      A missing branch = first run. Any other error raises (fail closed: better to stop
                      than to re-archive everything).
  apply_manifest()    after an upload: add that bundle's threads to committed.db
  push(data_dir)      save committed.db to the branch

Remote: STATE_REMOTE (any git URL), else https://x-access-token:$GITHUB_TOKEN@github.com/$GITHUB_REPOSITORY.git
Without either, everything is skipped (local runs).
"""
import gzip
import os
import shutil
import sqlite3
import subprocess
import tempfile
import time

from archiver import log, log_error
from state import State, now_ms

FILE = "state.db.gz"
ITEM_FILE = "item.json"   # {"n": current item number, "count": bundles already in it}


def _branch() -> str:
    return os.environ.get("STATE_BRANCH") or "futaba-state"


def remote_url():
    r = os.environ.get("STATE_REMOTE")
    if r:
        return r
    repo, tok = os.environ.get("GITHUB_REPOSITORY"), os.environ.get("GITHUB_TOKEN")
    if repo and tok:
        return f"https://x-access-token:{tok}@github.com/{repo}.git"
    return None


def _git(args, cwd):
    p = subprocess.run(
        ["git", "-c", "user.name=futaba-bot", "-c", "user.email=futaba-bot@users.noreply.github.com", *args],
        cwd=cwd, capture_output=True, text=True)
    return p


def _clean(text: str) -> str:
    tok = os.environ.get("GITHUB_TOKEN")
    return text.replace(tok, "***") if tok else text


def restore(data_dir: str) -> str:
    working = os.path.join(data_dir, "state.db")
    if os.path.exists(working):
        return "kept existing working state"
    url = remote_url()
    if not url:
        return "no state remote configured - starting with empty state"
    tmp = tempfile.mkdtemp(prefix="futaba-state-")
    try:
        _git(["init", "-q"], tmp)
        f = _git(["fetch", "-q", "--depth=1", url, f"refs/heads/{_branch()}"], tmp)
        if f.returncode != 0:
            if "couldn't find remote ref" in f.stderr:
                return f"no saved state yet (branch {_branch()} missing) - first run"
            raise RuntimeError("could not fetch saved state: " + _clean(f.stderr.strip()))
        show = subprocess.run(["git", "show", f"FETCH_HEAD:{FILE}"], cwd=tmp, capture_output=True)
        if show.returncode != 0:
            raise RuntimeError(f"saved state branch has no {FILE}")
        raw = gzip.decompress(show.stdout)
        os.makedirs(data_dir, exist_ok=True)
        with open(os.path.join(data_dir, "committed.db"), "wb") as f_:
            f_.write(raw)
        shutil.copyfile(os.path.join(data_dir, "committed.db"), working)
        item = subprocess.run(["git", "show", f"FETCH_HEAD:{ITEM_FILE}"], cwd=tmp, capture_output=True)
        if item.returncode == 0:
            with open(os.path.join(data_dir, ITEM_FILE), "wb") as f_:
                f_.write(item.stdout)
        st = State(working)
        n = st.db.execute("SELECT COUNT(*) FROM archived").fetchone()[0]
        st.close()
        return f"restored saved state: {n} archived thread(s)"
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def apply_manifest(data_dir: str, manifest_path: str, ia_item=None, ia_file=None) -> int:
    rows = []
    t = now_ms()
    with open(manifest_path, encoding="utf-8") as f:
        for line in f:
            hp, tid, replies = line.rstrip("\n").split("\t")
            host, _, path = hp.partition("/")
            rows.append({"host": host, "path": "/" + path, "thread_id": tid, "replies": int(replies), "archived_at": t,
                         "ia_item": ia_item, "ia_file": ia_file})
    st = State(os.path.join(data_dir, "committed.db"))
    try:
        st.import_archived(rows)
        st.db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    finally:
        st.close()
    return len(rows)


def push(data_dir: str) -> bool:
    url = remote_url()
    committed = os.path.join(data_dir, "committed.db")
    if not url or not os.path.exists(committed):
        return True
    tmp = tempfile.mkdtemp(prefix="futaba-state-")
    try:
        snap = os.path.join(tmp, "snap.db")
        src, dst = sqlite3.connect(committed), sqlite3.connect(snap)
        src.backup(dst)
        dst.close()
        src.close()
        with open(snap, "rb") as fi, gzip.open(os.path.join(tmp, FILE), "wb", compresslevel=9) as fo:
            shutil.copyfileobj(fi, fo)
        os.remove(snap)
        if os.path.exists(os.path.join(data_dir, ITEM_FILE)):
            shutil.copyfile(os.path.join(data_dir, ITEM_FILE), os.path.join(tmp, ITEM_FILE))
        _git(["init", "-q"], tmp)
        _git(["checkout", "-q", "-b", _branch()], tmp)
        _git(["add", "-A"], tmp)
        c = _git(["commit", "-q", "-m", "futaba state"], tmp)
        if c.returncode != 0:
            log_error("state commit failed: " + _clean(c.stderr.strip()))
            return False
        for attempt in range(1, 5):
            p = _git(["push", "-q", "--force", url, f"HEAD:refs/heads/{_branch()}"], tmp)
            if p.returncode == 0:
                log(f"saved state to branch {_branch()} ({os.path.getsize(os.path.join(tmp, FILE)) / 1e6:.1f} MB)")
                return True
            log_error(f"state push attempt {attempt}/4 failed: {_clean(p.stderr.strip())[:300]}")
            time.sleep(5 * attempt)
        return False
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
