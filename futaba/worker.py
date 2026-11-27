#!/usr/bin/env python3
"""
Futaba archive worker: ONE long-running job (GitHub-hosted runner), looping until RUN_HOURS is used up.

Loop:
  pass  = visit every board (lead boards first, CYCLE_LEADS rotation), stream every new/grown thread
          straight into the one growing bundle (current/bundle.zip.part on the runner's disk).
  then  = if the pass took less than PASS_INTERVAL_SECONDS (300), sit idle until 5 minutes after it
          started; otherwise start the next pass immediately.
  Whenever the bundle reaches BUNDLE_MAX_GB it is sealed (-> ready/<id>/) and uploader.py is started in
  the background to send it to Internet Archive; archiving carries on into a fresh bundle meanwhile.
  (Only one uploader runs at a time; a new one is started when the previous finished.)

At the end (RUN_HOURS, default 5, or a cancel signal): seal whatever is in the bundle, even if small, wait
for the uploads to finish and exit. The next cron-started job continues from the saved state.

  python worker.py            the loop above
  python worker.py --flush    seal the current bundle and upload everything that is left (used as a
                              last-chance step after cancels/crashes), then exit

Settings (env):
  DATA_DIR              folder on the runner's disk                              [./data]
  RUN_HOURS             how long this job archives before flushing and exiting   [5]
  PASS_INTERVAL_SECONDS minimum time between the START of two passes             [300]
  BUNDLE_MAX_GB         seal + upload at this bundle size                        [5]
  MIN_FREE_GB           stop starting threads if the disk has less than this free [2]
  MAX_THREADS_PER_RUN / MAX_THREADS_PER_BOARD  per pass / per board, 0 = unlimited [0 / 0]
  BUNDLE_COMPACT_MIN_MB  rewrite the zip at seal time when replaced copies wasted this much  [50]
  IA_ITEM_PREFIX / IA_BUNDLES_PER_ITEM / IA_ITEM_START: see uploader.py
  plus archiver.py's scraping options (MIN_REPLIES, THREAD_CONCURRENCY, CYCLE_LEADS, PRIORITY_BOARDS, ...)
"""
import fcntl
import os
import random
import shutil
import signal
import subprocess
import sys
import threading
import time

import http_util
import state_sync
from archiver import (SORT_CHOICES, Config, archive_thread, board_slug, env_float, env_int,
                      fetch_catalog_entries, fmt_bytes, load_config, log, log_error, order_boards, pick_threads)
from bundle import Bundle
from state import State

HERE = os.path.dirname(os.path.abspath(__file__))
GB = 1024 ** 3
stop = threading.Event()


def _on_signal(signum, _frame):
    log(f"signal {signum} received - finishing the current thread, then stopping")
    stop.set()


class Uploads:
    """Starts uploader.py in the background; at most one at a time."""

    def __init__(self):
        self.proc = None

    def kick(self) -> None:
        if self.proc is not None and self.proc.poll() is None:
            return  # one is running; it rescans for new bundles when it finishes its current one
        if self.proc is not None and self.proc.returncode not in (0, None):
            log_error(f"previous upload process exited with {self.proc.returncode}; its bundle stays on disk and is retried")
        self.proc = subprocess.Popen([sys.executable, os.path.join(HERE, "uploader.py")])

    def busy(self) -> bool:
        return self.proc is not None and self.proc.poll() is None

    def wait(self) -> None:
        if self.proc is not None:
            self.proc.wait()


class Run:
    def __init__(self, cfg: Config, state: State, bundle: Bundle, max_bytes: int, min_free: int, uploads: Uploads):
        self.cfg, self.state, self.bundle = cfg, state, bundle
        self.max_bytes, self.min_free, self.uploads = max_bytes, min_free, uploads
        self.lock = threading.Lock()  # guards the bundle file and the counters
        self.added = 0
        self.full_logged = False
        self.blacklisted = set()  # (host, path, id) of threads skipped by BLACKLIST this job: not fetched again

    def can_continue(self) -> bool:
        if stop.is_set():
            return False
        if shutil.disk_usage(self.cfg.data_dir).free < self.min_free:
            if not self.full_logged:
                log_error(f"disk has less than {fmt_bytes(self.min_free)} free (an upload is probably still "
                          f"running) - not starting new threads until it is freed")
                self.full_logged = True
            return False
        self.full_logged = False
        return True

    def seal_and_upload(self, why: str) -> None:
        """Call with self.lock held (or when no worker threads are running)."""
        dest = self.bundle.seal()
        if dest:
            log(f"bundle sealed ({why}) -> {dest}; uploading in the background, archiving continues in a new bundle")
            self.uploads.kick()
        self.bundle.open()


def archive_one(board, pick, run: Run) -> str:
    cfg = run.cfg
    label = f"{board.host}{board.path}{pick['id']}"
    abort = http_util.Abort(cfg.thread_timeout_seconds, f"thread timeout after {cfg.thread_timeout_seconds}s")
    try:
        out = archive_thread(board, pick["id"], cfg, abort, build=False)
    except Exception as e:  # noqa: BLE001
        log_error(f"{label} failed: {e or type(e).__name__}")
        return "failed"
    if out.blacklisted:
        run.blacklisted.add((board.host, board.path, pick["id"]))
        log(f"{label} skipped: {out.reason}")
        return "blacklisted"
    if not out.ok or not out.entries:
        log_error(f"{label} failed: {out.reason}")
        return "failed"

    prefix = f"{board_slug(board).strip('_')}/{pick['id']}_r{pick['replies']}/"
    try:
        with run.lock:  # streaming append: one thread at a time
            n = run.bundle.add_thread(f"{board.host}{board.path}", pick["id"], pick["replies"], prefix, out.entries)
            run.state.record_archived(board.host, board.path, pick["id"], pick["replies"])
            run.added += 1
            total = run.bundle.size
            if total >= run.max_bytes:
                run.seal_and_upload("size limit reached")
    except Exception as e:  # noqa: BLE001
        log_error(f"{label} could not be written to the bundle: {e}")
        return "failed"
    if run.bundle.last_replaced:
        old, freed = run.bundle.last_replaced
        log(f"{label} replaced its older copy ({old} replies) in the open bundle ({fmt_bytes(freed)} dropped)")
    filtered = f", {out.replies_removed} replies filtered" if out.replies_removed else ""
    log(f"{label} -> bundle ({pick['replies']} replies{filtered}, {out.asset_count} assets, +{fmt_bytes(n)}, "
        f"bundle {fmt_bytes(total)} / {fmt_bytes(run.max_bytes)})")
    return "built"


def process_board(board, run: Run, deadline: float, remaining: int) -> dict:
    cfg = run.cfg
    label = f"{board.host}{board.path}"
    sort_param, sort_label = random.choice(SORT_CHOICES)
    summary = {"board": label, "built": 0, "failed": 0, "blacklisted": 0}
    try:
        entries = fetch_catalog_entries(board, sort_param, cfg)
    except Exception as e:  # noqa: BLE001
        summary["error"] = str(e) or type(e).__name__
        log_error(f"[{label}] {summary['error']}")
        return summary

    candidates = [e for e in entries if e["replies"] >= cfg.min_replies][: cfg.max_candidates_per_board]
    limit = min(cfg.max_threads_per_board or 10 ** 9, remaining)
    if run.blacklisted:
        candidates = [e for e in candidates if (board.host, board.path, e["id"]) not in run.blacklisted]
    picked = pick_threads(board, candidates, cfg, run.state, limit)

    nxt = {"i": 0}
    lock = threading.Lock()

    def worker():
        while True:
            if time.time() >= deadline or not run.can_continue():
                return
            with lock:
                i = nxt["i"]
                nxt["i"] += 1
            if i >= len(picked):
                return
            result = archive_one(board, picked[i], run)
            with lock:
                summary[result] += 1

    threads = [threading.Thread(target=worker, daemon=True) for _ in range(min(cfg.thread_concurrency, len(picked)))]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    log(f"[{label}] sort={sort_label} catalog={len(entries)} candidates={len(candidates)} "
        f"picked={len(picked)} built={summary['built']} failed={summary['failed']} blacklisted={summary['blacklisted']}")
    return summary


def wait_until(t: float) -> None:
    while not stop.is_set() and time.time() < t:
        time.sleep(min(1.0, max(0.0, t - time.time())))


def finish(run: Run, uploads: Uploads, why: str) -> int:
    """Seal what is left, let the uploads finish, report."""
    with run.lock:
        if run.bundle.thread_count:
            run.seal_and_upload(why)
    uploads.wait()
    # The background uploader may have exited before the last seal; one synchronous pass catches the rest.
    rc = subprocess.run([sys.executable, os.path.join(HERE, "uploader.py")]).returncode
    return rc


def main() -> int:
    flush_only = "--flush" in sys.argv[1:]
    os.environ.setdefault("BOARDS", "all")
    try:
        cfg = load_config(require_ia=False)
    except Exception as e:  # noqa: BLE001
        log_error(f"configuration error: {e}")
        return 1
    os.makedirs(cfg.data_dir, exist_ok=True)

    lock_f = open(os.path.join(cfg.data_dir, "archive.lock"), "w")
    try:
        fcntl.flock(lock_f, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        log("another archive worker is already running on this disk - exiting")
        return 0

    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)

    try:
        log(state_sync.restore(cfg.data_dir))
    except Exception as e:  # noqa: BLE001
        log_error(f"{e} - stopping rather than re-archiving everything")
        return 1

    max_bytes = round(env_float("BUNDLE_MAX_GB", 5) * GB)
    min_free = round(env_float("MIN_FREE_GB", 2) * GB)
    run_seconds = env_float("RUN_HOURS", 5) * 3600
    interval = env_float("PASS_INTERVAL_SECONDS", 300)
    started = time.time()
    job_deadline = started + run_seconds

    state = State(cfg.db_path)
    bundle = Bundle(cfg.data_dir)
    bundle.open()
    uploads = Uploads()
    run = Run(cfg, state, bundle, max_bytes, min_free, uploads)
    log(f"bundle {bundle.id}: {bundle.thread_count} thread(s), {fmt_bytes(bundle.size)} / {fmt_bytes(max_bytes)}; "
        f"job runs {run_seconds / 3600:.2f} h, pass interval {interval:.0f}s")

    if flush_only:
        rc = finish(run, uploads, "flush")
        state.close()
        return rc

    uploads.kick()  # anything left sealed from an earlier crash goes out right away
    passes = 0
    while not stop.is_set() and time.time() < job_deadline:
        pass_start = time.time()
        passes += 1
        cycle_index = state.next_cycle() if cfg.cycle_groups else 0
        boards = order_boards(cfg, cycle_index)
        lead = ""
        if cfg.cycle_groups:
            g = cfg.cycle_groups[cycle_index % len(cfg.cycle_groups)]
            lead = " (leading with " + ", ".join(b.host + b.path for b in g) + ")" if g else " (no lead boards)"
        log(f"=== pass {passes}: {len(boards)} board(s){lead}; bundle {fmt_bytes(bundle.size)} / {fmt_bytes(max_bytes)}")
        added0, failed, visited = run.added, 0, 0
        remaining = cfg.max_threads_per_run or 10 ** 9
        for board in boards:
            if remaining <= 0 or time.time() >= job_deadline or not run.can_continue():
                break
            s = process_board(board, run, job_deadline, remaining)
            visited += 1
            failed += s["failed"]
            remaining -= s["built"] + s["failed"]
        took = time.time() - pass_start
        log(f"=== pass {passes} done: visited {visited}/{len(boards)}, +{run.added - added0} thread(s), "
            f"{failed} failed, {took:.0f}s; bundle {fmt_bytes(bundle.size)} / {fmt_bytes(max_bytes)}"
            + ("; upload running" if uploads.busy() else ""))
        uploads.kick()  # cheap: exits at once if nothing is sealed
        if took < interval:
            log(f"idle for {interval - took:.0f}s until the next pass")
            wait_until(pass_start + interval)

    reason = "cancel signal" if stop.is_set() else f"{run_seconds / 3600:.1f} h limit reached"
    log(f"stopping ({reason}); sealing the last bundle and finishing uploads")
    rc = finish(run, uploads, "end of job")
    state.close()
    return rc


if __name__ == "__main__":
    try:
        code = main()
    except Exception as e:  # noqa: BLE001
        log_error(f"unhandled error: {e}")
        code = 1
    sys.stdout.flush()
    sys.exit(code)
