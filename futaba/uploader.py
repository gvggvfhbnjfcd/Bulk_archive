#!/usr/bin/env python3
"""
Upload sealed bundles (DATA_DIR/ready/<id>/) to the Internet Archive.

Items: bundles are added to  <IA_ITEM_PREFIX><N>  (default bulk-futaba-archive1, ...). After
IA_BUNDLES_PER_ITEM (7) bundles have been added to an item, the next bundle starts item N+1.
Each bundle adds two files to its item:
    futaba_<bundle id>.zip            the big bundle (every thread inside)
    futaba_<bundle id>_manifest.txt   host/path <TAB> thread_id <TAB> replies

The counter {"n": N, "count": bundles in item N} is kept in DATA_DIR/item.json and saved to the state
branch with the archived-thread state, so it carries over from job to job. With no saved counter the first
item is IA_ITEM_START (default 1). A bundle's item is fixed the first time it is attempted (progress.json),
so retries never move it, and the counter only advances once IA accepted the bundle.

This runs as its own process, so archiving keeps going while a (possibly long) upload is in progress.
Only one uploader runs at a time (file lock). Progress per file is remembered, so a failed manifest upload
never re-sends GBs. A bundle directory is deleted only after IA accepted every file.

Env: IA_ACCESS_KEY, IA_SECRET_KEY (required), DATA_DIR, IA_ITEM_PREFIX [bulk-futaba-archive],
IA_BUNDLES_PER_ITEM [7], IA_ITEM_START [1], IA_COLLECTION [opensource], IA_MEDIATYPE [data],
IA_UPLOAD_TIMEOUT_SECONDS [7200], IA_ATTEMPTS [6], IA_RETRY_BASE_MS [30000], IA_CHECK_LIMIT,
IA_OVERLOAD_WAIT_SECONDS, IA_OVERLOAD_POLL_SECONDS, IA_VERIFY, IA_PROXY, IA_ENDPOINT.
Exit code 0 = nothing left / all uploaded, 1 = something failed (stays on disk, retried next time).
"""
import fcntl
import json
import re
import os
import shutil
import sys
import time

from archiver import env_int, env_on, env_float, fmt_bytes, log, log_error, parse_proxy_list
from bundle import file_stem, list_ready
import state_sync
from ia import IaConfig, upload_bundle_file


def load_ia() -> IaConfig:
    key, secret = os.environ.get("IA_ACCESS_KEY", ""), os.environ.get("IA_SECRET_KEY", "")
    if not key or not secret:
        raise RuntimeError("IA_ACCESS_KEY / IA_SECRET_KEY are not set (repo secrets)")
    return IaConfig(
        access_key=key, secret_key=secret,
        endpoint=(os.environ.get("IA_ENDPOINT") or "https://s3.us.archive.org").rstrip("/"),
        collection=os.environ.get("IA_COLLECTION") or "opensource",
        mediatype=os.environ.get("IA_MEDIATYPE") or "data",
        item_prefix="", shard="none",
        timeout_s=max(60, env_int("IA_UPLOAD_TIMEOUT_SECONDS", 7200)),
        check_limit=env_on("IA_CHECK_LIMIT", True),
        overload_wait_s=env_float("IA_OVERLOAD_WAIT_SECONDS", 120),
        overload_poll_s=max(0.05, env_float("IA_OVERLOAD_POLL_SECONDS", 15)),
        verify_md5=env_on("IA_VERIFY", True),
        log=log,
    )


def _prefix() -> str:
    p = re.sub(r"[^a-z0-9_.-]+", "-", (os.environ.get("IA_ITEM_PREFIX") or "bulk-futaba-archive").lower()).strip("-.")
    return p or "bulk-futaba-archive"


def load_counter(data_dir: str) -> dict:
    try:
        with open(os.path.join(data_dir, state_sync.ITEM_FILE)) as f:
            c = json.load(f)
        return {"n": int(c["n"]), "count": int(c["count"])}
    except (OSError, ValueError, KeyError, TypeError):
        return {"n": max(1, env_int("IA_ITEM_START", 1)), "count": 0}


def pick_item(data_dir: str) -> str:
    """The item the next NEW bundle goes to: the current one, or the next number once it holds
    IA_BUNDLES_PER_ITEM bundles."""
    c = load_counter(data_dir)
    if c["count"] >= max(1, env_int("IA_BUNDLES_PER_ITEM", 7)):
        c = {"n": c["n"] + 1, "count": 0}
    return f"{_prefix()}{c['n']}"


def note_bundle_added(data_dir: str, item: str) -> None:
    """Count a successfully uploaded bundle against its item (and make that item the current one)."""
    n = int(item[len(_prefix()):])
    c = load_counter(data_dir)
    c = {"n": n, "count": (c["count"] if c["n"] == n else 0) + 1}
    tmp = os.path.join(data_dir, state_sync.ITEM_FILE + ".tmp")
    with open(tmp, "w") as f:
        json.dump(c, f)
    os.replace(tmp, os.path.join(data_dir, state_sync.ITEM_FILE))


def upload_one(cfg: IaConfig, data_dir: str, bdir: str, attempts: int, base_s: float, proxies: list) -> bool:
    bid = open(os.path.join(bdir, "SEALED")).read().strip()
    stem = file_stem(bid)
    prog_path = os.path.join(bdir, "progress.json")
    done = json.load(open(prog_path)) if os.path.exists(prog_path) else {}
    if not done.get("item"):            # fixed on the first attempt, never changes on retries
        done["item"] = pick_item(data_dir)
        json.dump(done, open(prog_path, "w"))
    ident = done["item"]
    meta = {
        "collection": cfg.collection, "mediatype": cfg.mediatype,
        "title": os.environ.get("IA_ITEM_TITLE") or f"Futaba bulk archive {ident[len(_prefix()):]}",
        "description": os.environ.get("IA_ITEM_DESCRIPTION") or (
            "Futaba Channel threads, many threads per ZIP (futaba_<date>.zip). Each thread is a folder "
            "<board>/<threadid>_r<replies>/ with index.html + assets/. Every ZIP has a matching "
            "_manifest.txt listing host/path, thread id and reply count."),
        "subject": "futaba;2chan;imageboard;archive",
    }
    files = [(f"{stem}.zip", os.path.join(bdir, "bundle.zip")),                # big one first
             (f"{stem}_manifest.txt", os.path.join(bdir, "manifest.txt"))]
    proxy = None
    for name, path in files:
        if done.get(name) is True:
            continue
        size = os.path.getsize(path)
        for attempt in range(1, attempts + 1):
            log(f"uploading {name} ({fmt_bytes(size)}) to item {ident} - attempt {attempt}/{attempts}")
            t0 = time.time()
            r = upload_bundle_file(cfg, ident, path, name, meta, proxy=proxy)
            if r.ok:
                log(f"  done in {time.time() - t0:.0f}s ({fmt_bytes(size / max(1, time.time() - t0))}/s)")
                done[name] = True
                json.dump(done, open(prog_path, "w"))
                break
            log_error(f"  {name}: {r.reason}")
            if r.fatal:
                log_error("IA rejected our credentials (401/403) - check IA_ACCESS_KEY / IA_SECRET_KEY")
                return False
            if not r.retryable:
                return False
            if attempt < attempts:
                # SlowDown / overload: back off longer
                time.sleep(base_s * attempt * (2 if r.overloaded or r.status in (429, 503) else 1))
                if proxies and r.status in (429, 503):
                    proxy = proxies[(attempt - 1) % len(proxies)]
        else:
            return False
    # Save the threads of this bundle as "archived" for future jobs, only now that IA has them.
    try:
        note_bundle_added(data_dir, ident)
        n = state_sync.apply_manifest(data_dir, os.path.join(bdir, "manifest.txt"), ident, f"{stem}.zip")
        state_sync.push(data_dir)
        log(f"state: {n} thread(s) of {bid} recorded as archived")
    except Exception as e:  # noqa: BLE001
        log_error(f"could not save state after upload (will be re-saved next time): {e}")
    shutil.rmtree(bdir)
    c = load_counter(data_dir)
    log(f"bundle {bid} fully uploaded -> https://archive.org/details/{ident} ({c['count']} bundle(s) in that item); local copy deleted")
    return True


def main() -> int:
    data_dir = (os.environ.get("DATA_DIR") or "data").rstrip("/")
    os.makedirs(data_dir, exist_ok=True)
    lock_f = open(os.path.join(data_dir, "upload.lock"), "w")
    try:
        fcntl.flock(lock_f, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        log("another upload is already running - nothing to do")
        return 0
    if not list_ready(data_dir):
        log("no sealed bundles waiting for upload")
        return 0
    try:
        cfg = load_ia()
    except RuntimeError as e:
        log_error(str(e))
        return 1
    attempts = max(1, env_int("IA_ATTEMPTS", 6))
    base_s = env_int("IA_RETRY_BASE_MS", 30000) / 1000
    proxies = parse_proxy_list(os.environ.get("IA_PROXY", ""))
    ok = True
    failed = set()
    while True:  # rescan: the archiver may seal another bundle while we upload
        todo = [b for b in list_ready(data_dir) if b not in failed]
        if not todo:
            break
        for bdir in todo:
            if not upload_one(cfg, data_dir, bdir, attempts, base_s, proxies):
                failed.add(bdir)
                ok = False
    return 0 if ok else 1


if __name__ == "__main__":
    try:
        code = main()
    except Exception as e:  # noqa: BLE001
        log_error(f"unhandled error: {e}")
        code = 1
    sys.stdout.flush()
    sys.exit(code)
