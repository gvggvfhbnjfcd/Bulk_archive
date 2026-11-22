#!/usr/bin/env python3
"""
One-time import of an old futaba state.db (from the Pterodactyl / EC2 archiver) into the saved state
that the GitHub worker restores at the start of every job (branch `futaba-state`, file state.db.gz).

Only the `archived` table is imported. The old `spool` table (threads that were built but never
uploaded) is deliberately ignored: the worker treats spooled threads as "already have it", so importing
them would hide threads that are NOT on the Internet Archive.

An existing futaba-state branch is fetched first and merged (reply counts never go down), so running
this after the workflow has already been running does not throw away its progress.

  python import_state.py /path/to/state.db --out state.db.gz     just write the file, push it yourself
  python import_state.py /path/to/state.db --push                merge + push to the branch

--push needs a git remote: STATE_REMOTE=<url> (e.g. git@github.com:you/repo.git, or an https URL with
a token), or GITHUB_REPOSITORY + GITHUB_TOKEN. Your local git credentials work with an ssh/https URL
that doesn't need a token only if you use --out and push by hand (see README).
"""
import argparse
import gzip
import os
import shutil
import sqlite3
import sys
import tempfile

import state_sync
from archiver import log, log_error
from state import State


def read_archived(src_path: str) -> list:
    """Work on a temporary copy (plus its -wal file if there is one) so the original is never touched
    and a read-only folder or a WAL-mode database is no problem."""
    tmp = tempfile.mkdtemp(prefix="futaba-old-")
    try:
        copy = os.path.join(tmp, "old.db")
        shutil.copyfile(src_path, copy)
        if os.path.exists(src_path + "-wal"):
            shutil.copyfile(src_path + "-wal", copy + "-wal")
        db = sqlite3.connect(copy)
        rows = db.execute("SELECT host, path, thread_id, replies, archived_at FROM archived").fetchall()
        db.close()
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    return [{"host": h, "path": p, "thread_id": t, "replies": r, "archived_at": a} for h, p, t, r, a in rows]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("state_db")
    ap.add_argument("--out", help="write the gzipped state here instead of pushing")
    ap.add_argument("--push", action="store_true", help="merge with the branch and push it")
    a = ap.parse_args()
    if not a.out and not a.push:
        ap.error("give --out FILE and/or --push")

    rows = read_archived(a.state_db)
    bad = [r for r in rows if r["host"] != r["host"].lower() or not r["path"].startswith("/")]
    log(f"read {len(rows)} archived thread(s) from {a.state_db}" + (f" ({len(bad)} odd rows)" if bad else ""))

    work = tempfile.mkdtemp(prefix="futaba-import-")
    try:
        if a.push:
            if not state_sync.remote_url():
                log_error("no remote: set STATE_REMOTE (or GITHUB_REPOSITORY + GITHUB_TOKEN)")
                return 1
            log(state_sync.restore(work))   # merge base: whatever the branch holds now (or nothing)
        committed = os.path.join(work, "committed.db")
        st = State(committed)
        st.import_archived(rows)            # never lowers an existing reply count
        n = st.db.execute("SELECT COUNT(*) FROM archived").fetchone()[0]
        st.db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        st.close()
        log(f"merged state holds {n} archived thread(s)")
        if a.out:
            with open(committed, "rb") as fi, gzip.open(a.out, "wb", compresslevel=9) as fo:
                shutil.copyfileobj(fi, fo)
            log(f"wrote {a.out} ({os.path.getsize(a.out) / 1e6:.1f} MB)")
        if a.push:
            if not state_sync.push(work):
                log_error("push failed")
                return 1
    finally:
        shutil.rmtree(work, ignore_errors=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
