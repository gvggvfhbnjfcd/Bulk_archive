#!/usr/bin/env python3
"""
Download every zip file linked from one or more tsumanne.net listing pages and
pack them into ONE bundle zip (ZIP_STORED, since the inner zips are already
compressed), plus a text manifest of the thread IDs it contains.

Usage:
    # single page
    python download_zips.py "https://tsumanne.net/si/all/1" --out downloads

    # a range of pages: https://tsumanne.net/si/all, /all/1, /all/2 ... /all/9
    python download_zips.py --base "https://tsumanne.net/si/all" --pages 0 9 --out downloads

    # every thread collected on exactly one day (finds the right listing pages
    # itself, using the data/YYYY/MM/DD/<id>/ folder date of each row)
    python download_zips.py --base "https://tsumanne.net/si/all" --date 2026-11-16 --out downloads

Why --date uses the folder date and not the "update" column:
    The listing is sorted by thread ID (newest first), NOT by update time. A
    thread that is still live keeps getting updated, so an old page can contain
    rows with today's update time. The folder date in each row's link
    (data/2026/11/20/<id>/) never changes and always goes down as the page
    number goes up, so it is safe to binary-search on.

Outputs (in --out):
    si_<minid>-<maxid>.zip   bundle of <id>.zip files   (si_<date>.zip with --date)
    si_<minid>-<maxid>.txt   manifest, one thread ID per line
Exit codes:
    0  success (possibly nothing new to bundle)
    1  a listing page could not be fetched, archive.org could not be checked,
       or --date found no threads at all for that day (refuses to advance)
    2  some zips failed transiently (timeouts, 5xx, invalid zip) - the run is
       incomplete and should be retried rather than advanced
"""

import argparse
import io
import re
import time
import zipfile
from datetime import date
from pathlib import Path
from typing import NamedTuple
from urllib.parse import urljoin, urlparse

import requests

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; zip-downloader/2.1)"}
DEFAULT_CUTOFF = "2026-07-18"  # page mode only: threads updated on/after this date are skipped
PERMANENT_STATUSES = {404, 410}


# --------------------------------------------------------------------------- #
# Listing pages
# --------------------------------------------------------------------------- #

def page_url_for(base: str, page: int) -> str:
    """page 0 -> base itself, page N -> base/N (matches tsumanne.net's pagination)."""
    return base if page == 0 else f"{base.rstrip('/')}/{page}"


def infer_link_base(page_url: str) -> str:
    """
    tsumanne.net pages declare a <base> tag (base: /si/) that all relative
    links resolve against, NOT the page's own URL. A page at
    https://tsumanne.net/si/all/7120 still has its zip links resolve
    against https://tsumanne.net/si/. This finds that base by cutting the
    path right before "/all".
    """
    parsed = urlparse(page_url)
    path = parsed.path
    idx = path.find("/all")
    if idx != -1:
        base_path = path[:idx] + "/"
    else:
        base_path = path.rsplit("/", 1)[0] + "/"
    return f"{parsed.scheme}://{parsed.netloc}{base_path}"


class Row(NamedTuple):
    thread_id: str
    created: date           # from the data/YYYY/MM/DD/<id>/ folder; monotonic with page number
    updated: date | None    # "update" column; NOT monotonic (live threads keep changing)
    has_zip: bool           # live threads have no zip link yet


DATA_PATH_RE = re.compile(r"data/(\d{4})/(\d{2})/(\d{2})/(\d+)/")
ZIP_RE = re.compile(r"zip\.php\?id=(\d+)")
ROW_DATE_RE = re.compile(r"(\d{4})/(\d{2})/(\d{2})\s+\d{2}:\d{2}:\d{2}")


def parse_rows(html: str) -> list[Row]:
    """One Row per table row. Each row is scanned on its own so fields stay paired."""
    rows = []
    for chunk in re.split(r"<tr\b", html)[1:]:
        dm = DATA_PATH_RE.search(chunk)
        if not dm:
            continue
        um = ROW_DATE_RE.search(chunk)
        updated = date(int(um.group(1)), int(um.group(2)), int(um.group(3))) if um else None
        rows.append(Row(
            thread_id=dm.group(4),
            created=date(int(dm.group(1)), int(dm.group(2)), int(dm.group(3))),
            updated=updated,
            has_zip=bool(ZIP_RE.search(chunk)),
        ))
    return rows


def fetch_rows(page_url: str) -> list[Row]:
    resp = requests.get(page_url, headers=HEADERS, timeout=30)
    if resp.status_code == 404:
        return []  # past the end of the listing
    resp.raise_for_status()
    return parse_rows(resp.text)


def find_zip_links(page_url: str, cutoff: date) -> tuple[list[str], list[str], int]:
    """
    Return (absolute zip URLs, thread IDs, skipped_count) for one listing page.
    Rows whose update date is on or after `cutoff` are skipped.
    """
    link_base = infer_link_base(page_url)
    ids: list[str] = []
    skipped = 0
    seen = set()
    for row in fetch_rows(page_url):
        if not row.has_zip or row.thread_id in seen:
            continue
        if row.updated is not None and row.updated >= cutoff:
            skipped += 1
            continue
        seen.add(row.thread_id)
        ids.append(row.thread_id)
    ids = sorted(ids, key=int, reverse=True)
    return [urljoin(link_base, f"zip.php?id={i}") for i in ids], ids, skipped


def find_day_rows(base: str, day: date, max_pages: int, get_rows=None) -> list[Row]:
    """
    All rows whose folder (collection) date is exactly `day`.

    The listing is sorted by thread ID, newest first, and the folder date goes
    with the ID, so it only goes down as the page number goes up. Uses
    exponential + binary search to find where `day` starts, then scans forward
    until the pages are older than `day`. Pages shift toward higher numbers as
    new threads appear, so scanning forward (starting one page early) is safe.
    """
    if get_rows is None:
        cache: dict[int, list] = {}

        def get_rows(p: int):
            if p not in cache:
                cache[p] = fetch_rows(page_url_for(base, p))
            return cache[p]

    def dates(p):
        return [r.created for r in get_rows(p)]

    def min_date(p):
        ds = dates(p)
        return min(ds) if ds else date.min  # empty page = past the end

    def max_date(p):
        ds = dates(p)
        return max(ds) if ds else date.min

    if min_date(0) <= day:
        start = 0
    else:
        lo, hi = 0, 1                      # invariant: min_date(lo) > day
        while hi < max_pages and min_date(hi) > day:
            lo, hi = hi, hi * 2
        hi = min(hi, max_pages)
        if min_date(hi) > day:
            return []                      # day is older than anything reachable
        while hi - lo > 1:
            mid = (lo + hi) // 2
            if min_date(mid) <= day:
                hi = mid
            else:
                lo = mid
        start = hi

    found: list[Row] = []
    seen = set()
    p = max(0, start - 1)
    while p <= max_pages:
        rows = get_rows(p)
        if not rows:
            break
        for r in rows:
            if r.created == day and r.thread_id not in seen:
                seen.add(r.thread_id)
                found.append(r)
        if max_date(p) < day:
            break
        p += 1
    return sorted(found, key=lambda r: int(r.thread_id), reverse=True)


# --------------------------------------------------------------------------- #
# State: what's already archived / known bad
# --------------------------------------------------------------------------- #

def fetch_archived_ids(identifier: str) -> set[str]:
    """
    Thread IDs already present as individual `<id>.zip` files in the IA item
    (legacy uploads from before bundling). A brand-new item yields an empty set.
    Threads inside bundles are NOT visible here; those come from archived_ids.txt.
    """
    resp = requests.get(f"https://archive.org/metadata/{identifier}", timeout=30)
    resp.raise_for_status()
    data = resp.json()

    ids = set()
    for f in data.get("files", []):
        m = re.match(r"^(\d+)\.zip$", f.get("name", ""))
        if m:
            ids.add(m.group(1))
    return ids


def load_id_file(path: Path) -> set[str]:
    if not path.exists():
        return set()
    return {line.strip() for line in path.read_text().splitlines() if line.strip().isdigit()}


def save_id_file(path: Path, ids: set[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(f"{i}\n" for i in sorted(ids, key=int)))


# --------------------------------------------------------------------------- #
# Downloading
# --------------------------------------------------------------------------- #

class PermanentFailure(Exception):
    """The server says this thread's zip does not exist (404/410)."""


def fetch_zip_bytes(url: str) -> bytes:
    """One attempt. Raises PermanentFailure on 404/410, other errors on anything else."""
    resp = requests.get(url, headers=HEADERS, timeout=60)
    if resp.status_code in PERMANENT_STATUSES:
        raise PermanentFailure(f"HTTP {resp.status_code}")
    resp.raise_for_status()
    data = resp.content
    if not zipfile.is_zipfile(io.BytesIO(data)):
        raise ValueError("response is not a valid zip (HTML error page?)")
    return data


def fetch_with_retry(url: str, max_attempts: int = 3) -> bytes:
    """Retry transient errors with 2s, 4s, ... backoff. PermanentFailure is not retried."""
    last_error: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            return fetch_zip_bytes(url)
        except PermanentFailure:
            raise
        except (requests.RequestException, ValueError) as e:
            last_error = e
            if attempt < max_attempts:
                backoff = 2 ** attempt
                print(f"    retry {attempt}/{max_attempts - 1} failed ({e}); waiting {backoff}s...")
                time.sleep(backoff)
    assert last_error is not None
    raise last_error


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #

def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("page_url", nargs="?", help="A single listing page URL")
    parser.add_argument("--base", help="Base listing URL to paginate over, e.g. https://tsumanne.net/si/all")
    parser.add_argument("--pages", nargs=2, type=int, metavar=("START", "END"),
                        help="Page range (inclusive) to fetch when using --base, e.g. 0 9")
    parser.add_argument("--date", metavar="YYYY-MM-DD",
                        help="Archive every thread collected on exactly this day, by folder date "
                             "(requires --base; finds the pages itself, so --pages is not needed)")
    parser.add_argument("--max-pages", type=int, default=20000,
                        help="Upper bound for the --date page search (default: 20000)")
    parser.add_argument("--out", default="downloads", help="Output directory for the bundle + manifest")
    parser.add_argument("--delay", type=float, default=0.5, help="Seconds to wait between downloads")
    parser.add_argument("--cutoff", default=DEFAULT_CUTOFF, metavar="YYYY-MM-DD",
                        help=f"Page mode only: skip threads updated on/after this date (default: {DEFAULT_CUTOFF})")
    parser.add_argument("--archive-identifier", default="tsumanne-si-archive",
                        help="IA item to check for legacy individual <id>.zip files")
    parser.add_argument("--no-archive-check", action="store_true",
                        help="Skip checking archive.org for already-archived thread IDs")
    parser.add_argument("--failed-ids-file", default="state/failed_ids.txt",
                        help="Persistent list of thread IDs the server reports as 404/410")
    args = parser.parse_args()

    if not args.page_url and not args.base:
        parser.error("Provide either a single page_url, --base with --pages, or --base with --date")
    if args.date and not args.base:
        parser.error("--date requires --base")

    try:
        cutoff = date.fromisoformat(args.cutoff)
    except ValueError:
        parser.error(f"--cutoff must be YYYY-MM-DD, got {args.cutoff!r}")
    target_day = None
    if args.date:
        try:
            target_day = date.fromisoformat(args.date)
        except ValueError:
            parser.error(f"--date must be YYYY-MM-DD, got {args.date!r}")

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    # ---- already-archived IDs: legacy individual files in the IA item -------
    archived_ids: set[str] = set()

    if not args.no_archive_check:
        print(f"Checking legacy individual files in IA item '{args.archive_identifier}'...")
        try:
            legacy = fetch_archived_ids(args.archive_identifier)
            print(f"  {len(legacy)} individually archived thread(s).")
            archived_ids |= legacy
        except requests.RequestException as e:
            # Fail closed: don't risk re-uploading duplicates.
            print(f"  [fail] could not check archive.org: {e}")
            raise SystemExit(1)

    failed_ids_path = Path(args.failed_ids_file)
    known_bad_ids = load_id_file(failed_ids_path)
    if known_bad_ids:
        print(f"Loaded {len(known_bad_ids)} known-missing (404/410) thread ID(s) — these will be skipped.")
    newly_bad: set[str] = set()

    # ---- decide which threads to fetch --------------------------------------
    work: list[tuple[str, str]] = []   # (zip_url, thread_id)
    seen_ids: set[str] = set()
    page_failures = 0

    if target_day:
        print(f"Looking for threads collected on {target_day.isoformat()} under {args.base}")
        day_rows: list[Row] = []
        try:
            day_rows = find_day_rows(args.base, target_day, args.max_pages)
        except requests.RequestException as e:
            print(f"  [fail] could not fetch listing pages: {e}")
            page_failures += 1
        else:
            if not day_rows:
                # A real day always has threads; zero means the search or the
                # page layout is broken. Refuse to let the workflow advance.
                print(f"  [fail] no threads at all found for {target_day.isoformat()} — "
                      f"refusing to treat this day as done")
                page_failures += 1

        with_zip = [r for r in day_rows if r.has_zip]
        live = [r for r in day_rows if not r.has_zip]
        print(f"  found {len(day_rows)} thread(s) dated {target_day.isoformat()}: "
              f"{len(with_zip)} with a zip, {len(live)} still live / no zip yet")
        if live:
            print("  note: threads without a zip link cannot be archived by this run: "
                  + ", ".join(r.thread_id for r in live[:20]) + (" ..." if len(live) > 20 else ""))

        link_base = infer_link_base(page_url_for(args.base, 0))
        for r in with_zip:
            seen_ids.add(r.thread_id)
            work.append((urljoin(link_base, f"zip.php?id={r.thread_id}"), r.thread_id))
    else:
        page_urls = []
        if args.page_url:
            page_urls.append(args.page_url)
        if args.base:
            start, end = args.pages or (0, 0)
            page_urls.extend(page_url_for(args.base, p) for p in range(start, end + 1))

        for page_url in page_urls:
            print(f"Scanning {page_url}")
            try:
                links, ids, skipped = find_zip_links(page_url, cutoff)
            except requests.RequestException as e:
                print(f"  [fail] could not fetch page: {e}")
                page_failures += 1
                continue
            print(f"  found {len(links)} zip link(s) eligible for archiving "
                  f"({skipped} skipped for being updated on/after {cutoff.isoformat()})")
            for zip_url, thread_id in zip(links, ids):
                if thread_id not in seen_ids:
                    seen_ids.add(thread_id)
                    work.append((zip_url, thread_id))

    # ---- download into the bundle -------------------------------------------
    tmp_bundle = out_dir / "bundle.partial.zip"
    bundled_ids: list[str] = []
    transient_failures: list[str] = []

    with zipfile.ZipFile(tmp_bundle, "w", compression=zipfile.ZIP_STORED, allowZip64=True) as bundle:
        for zip_url, thread_id in work:
            if thread_id in archived_ids:
                print(f"  [skip] {thread_id}.zip already archived")
                continue
            if thread_id in known_bad_ids:
                print(f"  [skip] {thread_id}.zip previously reported missing (404/410)")
                continue

            print(f"  [get]  {zip_url}")
            try:
                data = fetch_with_retry(zip_url)
            except PermanentFailure as e:
                print(f"  [gone] {thread_id}: {e} — recording as permanently unavailable")
                newly_bad.add(thread_id)
                continue
            except (requests.RequestException, ValueError) as e:
                print(f"  [fail] {thread_id}: {e} — transient, will not be recorded; run will be incomplete")
                transient_failures.append(thread_id)
                continue

            bundle.writestr(f"{thread_id}.zip", data, compress_type=zipfile.ZIP_STORED)
            bundled_ids.append(thread_id)
            time.sleep(args.delay)  # be polite

    # ---- persist permanent failures immediately (safe regardless of outcome) -
    if newly_bad:
        save_id_file(failed_ids_path, known_bad_ids | newly_bad)
        print(f"{len(newly_bad)} thread(s) returned 404/410 and were added to {failed_ids_path}: "
              f"{', '.join(sorted(newly_bad, key=int))}")

    incomplete = bool(page_failures or transient_failures)

    # ---- finalize bundle ----------------------------------------------------
    if not bundled_ids:
        tmp_bundle.unlink(missing_ok=True)
        print("Nothing new to bundle.")
    elif incomplete:
        # Leave the partial bundle for debugging; the non-zero exit stops the
        # workflow before upload, and the next run redoes this day.
        print(f"Run incomplete; leaving partial bundle at {tmp_bundle} ({len(bundled_ids)} thread(s)).")
    else:
        ids_sorted = sorted(bundled_ids, key=int)
        name = f"si_{target_day.isoformat()}" if target_day else f"si_{ids_sorted[0]}-{ids_sorted[-1]}"
        bundle_path = out_dir / f"{name}.zip"
        manifest_path = out_dir / f"{name}.txt"
        tmp_bundle.replace(bundle_path)
        manifest_path.write_text("".join(f"{i}\n" for i in ids_sorted))
        size_mb = bundle_path.stat().st_size / (1024 * 1024)
        print(f"Done. Bundled {len(ids_sorted)} thread(s) into {bundle_path} ({size_mb:.1f} MiB), "
              f"manifest at {manifest_path}.")

    if page_failures:
        print(f"{page_failures} page/day failure(s) — exiting 1 so the run is treated as incomplete.")
        raise SystemExit(1)
    if transient_failures:
        print(f"{len(transient_failures)} zip(s) failed transiently: "
              f"{', '.join(sorted(transient_failures, key=int))} — exiting 2 so the run is retried.")
        raise SystemExit(2)


if __name__ == "__main__":
    main()
