#!/usr/bin/env python3
"""
Futaba Channel thread archiver (Internet Archive only), pure Python.

Pipeline: board catalogs -> thread pages + assets -> ZIP -> spool (disk) -> Internet Archive.

  - ZIPs go straight to Internet Archive (IAS3) through the official `internetarchive` library
    (ia.py); before each upload IA's check_limit is asked whether it is rationing us.
  - "Already archived?" state lives in a local SQLite file (state.py).
  - If IA can't take an upload (outage, rate limit, ...), the finished ZIP is
    kept in a spool folder on disk and retried at the start of every later run.
    A ZIP is deleted only after IA accepted it, and the thread is recorded as
    archived only at that moment too.
  - Nothing listens on a port. main.py starts one run at a time.

Each run:
  1. Visits boards (CYCLE_LEADS group, then PRIORITY_BOARDS, then the rest shuffled),
     fetches each catalog with a randomly chosen sort order, and picks threads
     that are new or have grown enough since they were archived.
  2. Archives them (THREAD_CONCURRENCY at once) and writes each ZIP to the spool.
  3. At the same time, upload threads (IA_UPLOAD_CONCURRENCY) work through the spool:
     backlog from earlier runs first, then ZIPs as the download workers finish them.
     If the spool reaches SPOOL_MAX_GB, downloading waits for uploads to free space.
  4. Stops starting new threads when RUN_BUDGET_SECONDS is used up or the per-run
     thread cap is reached; uploads then finish whatever is queued.

ENVIRONMENT
  Required:
    IA_ACCESS_KEY, IA_SECRET_KEY
    BOARDS  - "all", or comma-separated "host/path/" specs
  Where things live:
    DATA_DIR [./data]            state.db and spool/ live here
    SPOOL_MAX_GB [10]            stop archiving new threads when this much is
                                 waiting to upload (protects the disk)
  Internet Archive:
    IA_COLLECTION [opensource], IA_MEDIATYPE [web], IA_ITEM_PREFIX [futaba-archive]
    IA_ITEM_SHARD [month]        "month": item per board per month
                                 (futaba-archive-<board>-YYYY-MM);
                                 "none": one item per board, as before
    IA_IDENTIFIER                if set, EVERYTHING goes into this one item
    IA_ITEM_TITLE, IA_ITEM_DESCRIPTION
    IA_UPLOAD_TIMEOUT_SECONDS [900], IA_ATTEMPTS [3] tries per zip per run
    MAX_UPLOAD_ATTEMPTS [12]     a zip IA rejects with a non-retryable error
                                 this many times is parked ("stuck")
    IA_ENDPOINT                  override https://s3.us.archive.org (testing)
    IA_CHECK_LIMIT [on]          before each upload ask IA (check_limit, via the library's
                                 s3_is_overloaded) whether it is rationing this key/item; if so,
                                 wait for it to clear instead of sending a PUT that would 503
    IA_OVERLOAD_WAIT_SECONDS [45]  how long one upload waits for that to clear; after that the
                                 attempt counts as a SlowDown (no PUT is sent, nothing is lost)
    IA_OVERLOAD_POLL_SECONDS [15]  gap between check_limit polls while waiting
    IA_VERIFY [on]               send Content-MD5 so IA rejects an upload whose bytes differ
    IA_RETRY_BASE_MS [5000]      wait before retrying a failed upload (x1, x4, x12);
                                 after a SlowDown it doubles each time instead
    UPLOAD_ORDER [largest]       which spooled ZIP uploads next: "largest" (biggest file first)
                                 or "oldest" (oldest first)
    IA_UPLOAD_CONCURRENCY [1]    uploads to IA at once (IA rate-limits per item)
    IA_MIN_INTERVAL_MS [1500]    minimum gap between upload starts; doubles (max 60s)
                                 after each SlowDown and shrinks again on success
    IA_SLOWDOWN_TRIP [6]         SlowDown/429 answers in a row before uploads pause
                                 for the rest of the run (other errors pause after 3)
    IA_PROXY                     proxy URL(s) for IA uploads only, comma-separated
                                 (http://user:pass@host:port). Unused until IA answers
                                 SlowDown IA_PROXY_AFTER_SLOWDOWNS times in one run; then
                                 uploads switch to the first proxy, and to the next one
                                 after another IA_PROXY_AFTER_SLOWDOWNS SlowDowns. Every
                                 run starts on the normal connection again.
    IA_PROXY_AFTER_SLOWDOWNS [2] SlowDowns (not necessarily in a row) before switching
  What to archive (defaults in brackets):
    PRIORITY_BOARDS              boards visited first every run
    CYCLE_LEADS                  alternate which boards lead each run. Groups split
                                 by ";", boards inside a group by ",". The word
                                 "none" = a turn with no lead boards. Lead boards
                                 go first, then PRIORITY_BOARDS, then the rest.
    MIN_REPLIES [3]
    BLACKLIST [empty]: comma-separated words; a thread whose text contains any of them is skipped,
        except a word with a Japanese character directly before or after it does not count
    BLACKLIST_ALWAYS [empty]: same, but these words are skipped even right next to Japanese text
    BLACKLIST_REPLY [empty]: a reply containing any of these words is left out of the archived page,
        together with its images; if the OPENING POST contains one, the whole thread is skipped.
        Same Japanese-text and slash exemptions as BLACKLIST
    MAX_THREADS_PER_RUN [60]     your main egress/cost dial
    MAX_THREADS_PER_BOARD [15]
    MAX_CANDIDATES_PER_BOARD [240]
    THREAD_CONCURRENCY [4]       threads in flight at once (each holds its whole
                                 ZIP in memory while building)
    ASSET_CONCURRENCY [4]        asset downloads in parallel per thread
    RUN_BUDGET_SECONDS [540]     stop starting new threads after this
    HARD_GRACE_SECONDS [900]     extra time for in-flight uploads before exit(2)
    THREAD_TIMEOUT_SECONDS [240]
    REARCHIVE_MIN_DELTA [20], REARCHIVE_GROWTH [0.5]
    MAX_ASSETS_PER_THREAD [75], MAX_ASSET_BYTES [0 = no limit]
    USER_AGENT, CXYL_COOKIE, OFFLOAD_BASE_URL, OFFLOAD_BASE_URL_F3
    SOURCE_SCHEME [https]        testing only

Exit code: 0 normally. 1 on a configuration error, when IA is refusing uploads,
or when every attempted thread failed. 2 on the hard deadline.

Commands:
  python archiver.py            one run
  python archiver.py --status   print counts from state.db and exit
"""
import json
import math
import os
import random
import re
import struct
import sys
import threading
import time
import zlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import unquote, urljoin, urlsplit

import http_util
from boards import ALL_BOARDS, Board
from ia import IaConfig, sanitize_ia_identifier_part, upload_to_ia
from state import SpoolRow, State, now_ms

HERE = os.path.dirname(os.path.abspath(__file__))


# ---------- Logging ----------

def _ts() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def log(msg: str) -> None:
    print(f"[{_ts()}] {msg}", flush=True)


def log_error(msg: str) -> None:
    print(f"[{_ts()}] ERROR {msg}", file=sys.stderr, flush=True)


def fmt_bytes(n: float) -> str:
    if n < 1024:
        return f"{int(n)} B"
    v = n / 1024
    units = ["KB", "MB", "GB", "TB"]
    i = 0
    while v >= 1024 and i < len(units) - 1:
        v /= 1024
        i += 1
    return f"{v:.1f} {units[i]}"


# ---------- Config ----------

def _env(name: str) -> str:
    return os.environ.get(name, "")


def env_int(name: str, fallback: int) -> int:
    m = re.match(r"\s*([+-]?\d+)", _env(name))
    if not m:
        return fallback
    n = int(m.group(1))
    return n if n >= 0 else fallback


def env_float(name: str, fallback: float) -> float:
    m = re.match(r"\s*([+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?)", _env(name))
    if not m:
        return fallback
    n = float(m.group(1))
    return n if n >= 0 else fallback


def env_on(name: str, fallback: bool) -> bool:
    v = _env(name).lower()
    if not v:
        return fallback
    return v not in ("off", "0", "false", "no")


def require_env(name: str) -> str:
    v = _env(name)
    if not v:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return v


@dataclass
class OffloadUploader:
    prefix: str  # e.g. "fu", "f3"
    base_url: str  # absolute, trailing-slash-normalised


@dataclass
class Config:
    boards: list
    priority_boards: list
    cycle_groups: list  # groups of lead boards; run N leads with group N % len
    scheme: str
    data_dir: str
    db_path: str
    spool_dir: str
    spool_max_bytes: int
    ia: Optional[IaConfig]
    ia_attempts: int
    ia_retry_base_ms: int
    ia_upload_concurrency: int
    ia_min_interval_ms: int
    ia_slowdown_trip: int
    ia_proxies: list
    ia_proxy_after: int
    max_upload_attempts: int
    min_replies: int
    max_threads_per_run: int
    max_threads_per_board: int
    max_candidates_per_board: int
    thread_concurrency: int
    asset_concurrency: int
    budget_seconds: int
    hard_grace_seconds: int
    thread_timeout_seconds: int
    rearchive_min_delta: int
    rearchive_growth: float
    max_assets_per_thread: int
    max_asset_bytes: int
    user_agent: str
    cxyl_cookie: str
    offload_uploaders: list = field(default_factory=list)
    blacklist: list = field(default_factory=list)  # compiled regexes, see parse_blacklist()
    blacklist_always: list = field(default_factory=list)  # BLACKLIST_ALWAYS: no Japanese-text exemption
    blacklist_reply: list = field(default_factory=list)   # BLACKLIST_REPLY: drops just the matching replies


def parse_board_spec(spec: str) -> Board:
    m = re.match(r"^([^/]+)(/.*)$", spec.strip())
    if not m:
        raise ValueError(f'Bad board spec "{spec}" - expected host/path/ such as may.2chan.net/b/')
    host = m.group(1)
    if host != host.lower():
        raise ValueError(f'Board host must be lowercase (state and IA identifiers are case-sensitive): "{host}"')
    path = m.group(2) if m.group(2).endswith("/") else m.group(2) + "/"
    return Board(host, path)


def resolve_boards() -> list:
    raw = _env("BOARDS").strip()
    if raw.lower() == "all":
        return list(ALL_BOARDS)
    specs = [s.strip() for s in raw.split(",") if s.strip()] if raw else []
    if not specs:
        host, path = _env("BOARD_HOST"), _env("BOARD_PATH")
        if host and path:
            specs.append(f"{host}{path}")
    if not specs:
        raise ValueError(
            'No boards configured - set BOARDS (e.g. "may.2chan.net/b/", comma-separated, or "all") '
            "or BOARD_HOST + BOARD_PATH"
        )
    return [parse_board_spec(s) for s in specs]


OFFLOAD_UPLOADER_DEFAULTS = [
    ("fu", "OFFLOAD_BASE_URL", "https://dec.2chan.net/up2/src/"),
    ("f3", "OFFLOAD_BASE_URL_F3", "https://dec.2chan.net/up/src/"),
]


def resolve_offload_uploaders() -> list:
    out = []
    for prefix, env_var, default in OFFLOAD_UPLOADER_DEFAULTS:
        configured = _env(env_var) or default
        out.append(OffloadUploader(prefix, configured if configured.endswith("/") else configured + "/"))
    return out


def parse_proxy_list(raw: str) -> list:
    out = []
    for p in re.split(r"[,\s]+", raw.strip()):
        if p:
            out.append(p if "://" in p else f"http://{p}")
    return out


def proxy_label(proxy: Optional[str]) -> str:
    """host:port only, so proxy credentials never reach the log."""
    if not proxy:
        return "direct connection"
    u = urlsplit(proxy)
    return f"{u.hostname}:{u.port}" if u.port else str(u.hostname)


def load_config(require_ia: bool = True) -> Config:
    data_dir = (_env("DATA_DIR") or os.path.join(HERE, "data")).rstrip("/") or "/"
    boards = resolve_boards()

    def only_configured(specs: str) -> list:
        parsed = [parse_board_spec(s) for s in specs.split(",") if s.strip()]
        return [p for p in parsed if p in boards]

    priority_raw = _env("PRIORITY_BOARDS").strip()
    priority = only_configured(priority_raw) if priority_raw else []

    cycle_raw = _env("CYCLE_LEADS").strip()
    cycle_groups = []
    if cycle_raw:
        for g in (x.strip() for x in cycle_raw.split(";")):
            if not g:
                continue
            cycle_groups.append([] if g.lower() == "none" else only_configured(g))

    ia = None
    if require_ia:
        ia = IaConfig(
            access_key=require_env("IA_ACCESS_KEY"),
            secret_key=require_env("IA_SECRET_KEY"),
            endpoint=(_env("IA_ENDPOINT") or "https://s3.us.archive.org").rstrip("/"),
            collection=_env("IA_COLLECTION") or "opensource",
            mediatype=_env("IA_MEDIATYPE") or "web",
            identifier=_env("IA_IDENTIFIER") or None,
            item_prefix=sanitize_ia_identifier_part(_env("IA_ITEM_PREFIX") or "futaba-archive") or "futaba-archive",
            shard="none" if (_env("IA_ITEM_SHARD") or "month").lower() == "none" else "month",
            title=_env("IA_ITEM_TITLE") or None,
            description=_env("IA_ITEM_DESCRIPTION") or None,
            timeout_s=max(30, env_int("IA_UPLOAD_TIMEOUT_SECONDS", 900)),
            check_limit=env_on("IA_CHECK_LIMIT", True),
            overload_wait_s=max(0.0, env_float("IA_OVERLOAD_WAIT_SECONDS", 45)),
            overload_poll_s=max(0.05, env_float("IA_OVERLOAD_POLL_SECONDS", 15)),
            verify_md5=env_on("IA_VERIFY", True),
            log=log,
        )

    return Config(
        boards=boards,
        priority_boards=priority,
        cycle_groups=cycle_groups,
        scheme=_env("SOURCE_SCHEME") or "https",
        data_dir=data_dir,
        db_path=f"{data_dir}/state.db",
        spool_dir=f"{data_dir}/spool",
        spool_max_bytes=round(env_float("SPOOL_MAX_GB", 10) * 1024 ** 3),
        ia=ia,
        ia_attempts=max(1, env_int("IA_ATTEMPTS", 3)),
        ia_retry_base_ms=max(0, env_int("IA_RETRY_BASE_MS", 5000)),
        ia_upload_concurrency=max(1, env_int("IA_UPLOAD_CONCURRENCY", 1)),
        ia_min_interval_ms=max(0, env_int("IA_MIN_INTERVAL_MS", 1500)),
        ia_slowdown_trip=max(1, env_int("IA_SLOWDOWN_TRIP", 6)),
        ia_proxies=parse_proxy_list(_env("IA_PROXY")),
        ia_proxy_after=max(1, env_int("IA_PROXY_AFTER_SLOWDOWNS", 2)),
        max_upload_attempts=max(1, env_int("MAX_UPLOAD_ATTEMPTS", 12)),
        min_replies=env_int("MIN_REPLIES", 3),
        max_threads_per_run=env_int("MAX_THREADS_PER_RUN", 60),
        max_threads_per_board=env_int("MAX_THREADS_PER_BOARD", 15),
        max_candidates_per_board=env_int("MAX_CANDIDATES_PER_BOARD", 240),
        thread_concurrency=max(1, env_int("THREAD_CONCURRENCY", 4)),
        asset_concurrency=max(1, env_int("ASSET_CONCURRENCY", 4)),
        budget_seconds=env_int("RUN_BUDGET_SECONDS", 540),
        hard_grace_seconds=env_int("HARD_GRACE_SECONDS", 900),
        thread_timeout_seconds=max(10, env_int("THREAD_TIMEOUT_SECONDS", 240)),
        rearchive_min_delta=env_int("REARCHIVE_MIN_DELTA", 20),
        rearchive_growth=env_float("REARCHIVE_GROWTH", 0.5),
        max_assets_per_thread=env_int("MAX_ASSETS_PER_THREAD", 75),
        max_asset_bytes=env_int("MAX_ASSET_BYTES", 0),
        user_agent=_env("USER_AGENT") or "FutabaThreadArchiver/1.0 (personal archive)",
        cxyl_cookie=_env("CXYL_COOKIE") or "100x100x0x0x0",
        offload_uploaders=resolve_offload_uploaders(),
        blacklist=parse_blacklist(_env("BLACKLIST")),
        blacklist_always=parse_blacklist(_env("BLACKLIST_ALWAYS")),
        blacklist_reply=parse_blacklist(_env("BLACKLIST_REPLY")),
    )


# ---------- Blacklist ----------

def parse_blacklist(spec: str) -> list:
    """BLACKLIST is a comma-separated list of words. Each becomes a case-insensitive regex that must
    not be glued to other ASCII letters/digits, so "D-P" matches "d-p" and "D-P!" but not "add-ps".
    (Japanese text has no spaces, so only ASCII neighbours count as being part of the same word.)"""
    out = []
    for word in spec.split(","):
        word = word.strip()
        if word:
            out.append(re.compile(r"(?<![A-Za-z0-9_])" + re.escape(word) + r"(?![A-Za-z0-9_])", re.I))
    return out


def visible_text(html: str) -> str:
    """The text a reader sees: no scripts, styles, comments or tags (so CSS like "background-position"
    or a file name inside an attribute can never match), with entities decoded."""
    import html as html_lib
    t = re.sub(r"<(script|style)\b.*?</\1\s*>", " ", html, flags=re.I | re.S)
    t = re.sub(r"<!--.*?-->", " ", t, flags=re.S)
    t = re.sub(r"<[^>]*>", " ", t)
    return html_lib.unescape(t)


_JAPANESE_CHAR = re.compile(r"[\u3005\u3040-\u30ff\u3400-\u4dbf\u4e00-\u9fff\uff66-\uff9f]")


def blacklisted_word(html: str, blacklist: list, always: list = ()) -> Optional[str]:
    """Return the first blacklisted word found in the page text, or None.
    `always` words (BLACKLIST_ALWAYS) count wherever they appear. `blacklist` words (BLACKLIST) do not
    count when a Japanese character (hiragana, katakana, kanji) is directly before OR after them,
    e.g. "これはDP" and "DPです" pass, while "DP" or "this is DP" is filtered. A word wrapped in slashes,
    like "/DP/", also does not count (`always` words still do)."""
    if not blacklist and not always:
        return None
    text = visible_text(html)
    for rx in always:
        m = rx.search(text)
        if m:
            return m.group(0)
    for rx in blacklist:
        for m in rx.finditer(text):
            before = text[m.start() - 1] if m.start() > 0 else ""
            after = text[m.end()] if m.end() < len(text) else ""
            if (before and _JAPANESE_CHAR.match(before)) or (after and _JAPANESE_CHAR.match(after)):
                continue
            if before == "/" and after == "/":  # a board path such as "/DP/" is not the word itself
                continue
            return m.group(0)
    return None


def opening_post_html(html: str) -> str:
    """The opening post: from the thread's <div class="thre"> up to the first reply table (or the end
    marker / end of page when there are no replies). The form, ads and footer around it are excluded."""
    m = re.search(r'<div\b[^>]*\bclass\s*=\s*["\']?thre\b', html, re.I)
    start = m.start() if m else 0
    end = len(html)
    marker = html.find("<!--スレッド終了-->", start)
    if marker != -1:
        end = marker
    for t in _TABLE_RE.finditer(html, start, end):
        if _REPLY_CELL_RE.search(t.group(0)):
            end = t.start()
            break
    return html[start:end]


# A Futaba reply is its own <table> whose reply cell has class "rtd"; the opening post is not in a table.
_TABLE_RE = re.compile(r"<table\b[^>]*>(?:(?!<table\b).)*?</table\s*>", re.I | re.S)
_REPLY_CELL_RE = re.compile(r"<td\b[^>]*\bclass\s*=\s*[\"']?rtd\b", re.I)


def strip_blacklisted_replies(html: str, blacklist: list) -> tuple:
    """Remove every reply whose text contains a BLACKLIST_REPLY word (same matching rules as BLACKLIST).
    Returns (html, number_removed). Done before assets are collected, so images that only appear in a
    removed reply are never downloaded. The opening post is never removed here."""
    if not blacklist:
        return html, 0
    removed = 0

    def drop(m):
        nonlocal removed
        block = m.group(0)
        if _REPLY_CELL_RE.search(block) and blacklisted_word(block, blacklist):
            removed += 1
            return ""
        return block

    return _TABLE_RE.sub(drop, html), removed


# ---------- Charset handling ----------

def _codec_for(label: str) -> Optional[str]:
    """Map a charset label to a Python codec. Shift_JIS means Windows-31J here,
    as in browsers (it covers the extra characters Futaba posts use)."""
    label = label.strip().strip("\"'").lower()
    if label in ("shift_jis", "shift-jis", "sjis", "x-sjis", "ms_kanji", "csshiftjis", "windows-31j", "cp932"):
        return "cp932"
    try:
        import codecs
        return codecs.lookup(label).name
    except LookupError:
        return None


def decode_buffer(data: bytes, content_type: Optional[str]) -> str:
    attempts = []
    if content_type:
        m = re.search(r"charset=([^;]+)", content_type, re.I)
        if m:
            attempts.append(m.group(1))
    attempts += ["shift_jis", "utf-8"]
    for label in attempts:
        codec = _codec_for(label)
        if codec:
            return data.decode(codec, errors="replace")
    return data.decode("utf-8", errors="replace")


_META_HTTP_EQUIV = re.compile(
    r"""<meta\s+http-equiv=["']?Content-type["']?\s+content=["']text/html;\s*charset=[^"'>]+["']\s*/?>""", re.I
)
_META_CHARSET = re.compile(r"""<meta\s+charset\s*=\s*["']?[^"'>\s]+["']?\s*/?>""", re.I)


def force_utf8_meta(html: str) -> str:
    """index.html is always written as fresh UTF-8, so a stale Shift_JIS declaration
    must be rewritten or a browser would mis-decode the Japanese text."""
    out = _META_HTTP_EQUIV.sub('<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">', html, count=1)
    return _META_CHARSET.sub('<meta charset="UTF-8">', out, count=1)


# ---------- Catalog ----------

_CATALOG_RE = re.compile(
    r"""<a\s+href=["']?res/(\d+)\.htm["']?[^>]*>[\s\S]*?</a>\s*<br>\s*<font[^>]*>(\d+)</font>""", re.I
)


def parse_catalog(html: str) -> list:
    return [{"id": m.group(1), "replies": int(m.group(2))} for m in _CATALOG_RE.finditer(html)]


# param None = no &sort= param at all (the site's own default ordering).
SORT_CHOICES = [(None, "default"), (1, "newest"), (2, "oldest"), (6, "most replies")]


def fetch_catalog_entries(board: Board, sort_param, cfg: Config) -> list:
    url = f"{cfg.scheme}://{board.host}{board.path}futaba.php?mode=cat" + (f"&sort={sort_param}" if sort_param is not None else "")
    res = http_util.request(
        url,
        headers={"User-Agent": cfg.user_agent, "Cookie": f"cxyl={cfg.cxyl_cookie}"},
        timeout=30,
        accept_gzip=True,
    )
    if not res.ok:
        raise RuntimeError(f"catalog fetch failed: {res.status}")
    return parse_catalog(decode_buffer(res.body or b"", res.headers.get("content-type")))


# ---------- Asset extraction ----------

ASSET_HREF_EXTENSIONS = re.compile(r"\.(jpe?g|png|gif|webp|bmp|webm|mp4|txt|mp3|mov|ogg|pdf|mht|html|zip)(\?|#|$)", re.I)
_ATTR_TAIL = r"""\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\s>]+))"""


def _first(m) -> Optional[str]:
    for g in m.groups():
        if g is not None:
            return g
    return None


def _attr_value(tag: str, attr: str) -> Optional[str]:
    m = re.search(r"\b" + attr + _ATTR_TAIL, tag, re.I)
    return _first(m) if m else None


def extract_asset_urls(html: str) -> list:
    """Content assets - post images, video, attached text files, the stylesheet -
    ignoring ad/tracking iframes and scripts."""
    urls = {}
    for m in re.finditer(r"<(?:img|video|source)\b[^>]*>", html, re.I):
        url = _attr_value(m.group(0), "src")
        if url:
            urls[url] = None
    for m in re.finditer(r"<link\b[^>]*>", html, re.I):
        tag = m.group(0)
        if re.search(r"""rel\s*=\s*["']?stylesheet""", tag, re.I):
            url = _attr_value(tag, "href")
            if url:
                urls[url] = None
    for m in re.finditer(r"\shref" + _ATTR_TAIL, html, re.I):
        url = _first(m)
        if url and ASSET_HREF_EXTENSIONS.search(url):
            urls[url] = None
    return list(urls)


_PAIR_RE = re.compile(
    r"""<a\b[^>]*\bhref\s*=\s*(["'])([^"']+)\1[^>]*>\s*<img\b[^>]*\bsrc\s*=\s*(["'])([^"']+)\3[^>]*>\s*</a>""", re.I
)


def extract_linked_thumbnails(html: str) -> dict:
    """Futaba wraps each posted image as <a href="FULL"><img src="THUMB"></a>.
    Fetching both doubles requests per image, so find the pairs and skip the
    thumbnail. Returns {thumb_raw_url: full_raw_url}."""
    out = {}
    for m in _PAIR_RE.finditer(html):
        full, thumb = m.group(2), m.group(4)
        if full and thumb and full != thumb and ASSET_HREF_EXTENSIONS.search(full):
            out[thumb] = full
    return out


def extract_stylesheet_urls(html: str) -> set:
    urls = set()
    for m in re.finditer(r"<link\b[^>]*>", html, re.I):
        tag = m.group(0)
        if re.search(r"""rel\s*=\s*["']?stylesheet""", tag, re.I):
            m2 = re.search(r"\bhref" + _ATTR_TAIL, tag, re.I)
            url = _first(m2) if m2 else None
            if url:
                urls.add(url)
    return urls


# ---------- Offload uploaders ----------

OFFLOAD_EXTENSIONS = "jpe?g|png|gif|webp|bmp|webm|mp4|txt|mp3|mov|ogg|pdf|mht|html|zip"


def _base(u: OffloadUploader) -> str:
    return u.base_url if u.base_url.endswith("/") else u.base_url + "/"


def extract_offloaded_asset_urls(html: str, uploaders: list) -> list:
    """Bare filenames like "fu6988461.jpg" need their uploader's base URL prepended."""
    urls = {}
    for u in uploaders:
        for m in re.finditer(rf"\b{u.prefix}\d+\.(?:{OFFLOAD_EXTENSIONS})\b", html, re.I):
            urls[f"{_base(u)}{m.group(0)}"] = None
    return list(urls)


def rewrite_offloaded_references(html: str, uploaders: list) -> str:
    """Rewrite src="fuNNN.ext" / href="fuNNN.ext" to the uploader's absolute URL, in attributes only."""
    out = html
    for u in uploaders:
        base = _base(u)
        pat = re.compile(rf"""(\b(?:src|href)\s*=\s*)(["'])({u.prefix}\d+\.(?:{OFFLOAD_EXTENSIONS}))\2""", re.I)
        out = pat.sub(lambda m: f"{m.group(1)}{m.group(2)}{base}{m.group(3)}{m.group(2)}", out)
    return out


def replace_attribute_values(html: str, value_map: dict) -> str:
    """Replace every quoted occurrence of any exact attribute value with its mapped value, in one pass."""
    if not value_map:
        return html
    alt = "|".join(re.escape(k) for k in value_map)
    pat = re.compile(rf"""(["'])({alt})\1""")
    return pat.sub(lambda m: f"{m.group(1)}{value_map[m.group(2)]}{m.group(1)}", html)


def local_filename_for(absolute_url: str, used: set) -> str:
    base = "asset"
    try:
        base = unquote(urlsplit(absolute_url).path.split("/")[-1]) or "asset"
    except Exception:
        pass
    base = re.sub(r"[^A-Za-z0-9._-]", "_", base) or "asset"
    candidate = base
    n = 1
    while candidate in used:
        dot = base.rfind(".")
        candidate = f"{base[:dot]}_{n}{base[dot:]}" if dot > 0 else f"{base}_{n}"
        n += 1
    used.add(candidate)
    return candidate


@dataclass
class FetchedAsset:
    raw: str
    absolute: str
    data: bytes


def fetch_assets(refs: list, user_agent: str, max_asset_bytes: int, concurrency: int, abort: http_util.Abort) -> list:
    """Fetch (raw, absolute) URL pairs with limited concurrency. Each request has its
    own timeout and everything stops when the thread-wide abort fires."""
    results = [None] * len(refs)
    counter = {"i": 0}
    lock = threading.Lock()

    def worker():
        while not abort.aborted:
            with lock:
                i = counter["i"]
                counter["i"] += 1
            if i >= len(refs):
                return
            raw, absolute = refs[i]
            try:
                res = http_util.request(
                    absolute, headers={"User-Agent": user_agent}, timeout=60, abort=abort, max_bytes=max_asset_bytes
                )
                if res.ok and not res.skipped and res.body is not None:
                    results[i] = FetchedAsset(raw, absolute, res.body)
            except Exception:
                pass  # asset fetch failed or timed out - the page just shows a broken image/video

    threads = [threading.Thread(target=worker, daemon=True) for _ in range(min(concurrency, len(refs)))]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return [r for r in results if r is not None]


# ---------- ZIP writer (store method, deflate for text entries) ----------

@dataclass
class ZipEntry:
    name: str
    data: bytes
    compress: bool = False  # deflate (method 8) instead of storing raw (method 0); only worth it for text


def _dos_datetime():
    d = datetime.now(timezone.utc)
    year = max(1980, d.year)
    t = (d.hour << 11) | (d.minute << 5) | (d.second >> 1)
    dt = ((year - 1980) << 9) | (d.month << 5) | d.day
    return t, dt


def _deflate_raw(data: bytes) -> bytes:
    c = zlib.compressobj(6, zlib.DEFLATED, -15)
    return c.compress(data) + c.flush()


def build_zip(entries: list) -> bytes:
    t, d = _dos_datetime()
    local, central = [], []
    offset = 0
    for e in entries:
        name = e.name.encode("utf-8")
        crc = zlib.crc32(e.data) & 0xFFFFFFFF
        method, stored = 0, e.data
        if e.compress:
            comp = _deflate_raw(e.data)
            if len(comp) < len(e.data):
                method, stored = 8, comp
        lh = struct.pack("<IHHHHHIIIHH", 0x04034B50, 20, 0, method, t, d, crc, len(stored), len(e.data), len(name), 0)
        local += [lh, name, stored]
        ch = struct.pack(
            "<IHHHHHHIIIHHHHHII", 0x02014B50, 20, 20, 0, method, t, d, crc, len(stored), len(e.data),
            len(name), 0, 0, 0, 0, 0, offset,
        )
        central += [ch, name]
        offset += len(lh) + len(name) + len(stored)
    central_blob = b"".join(central)
    eocd = struct.pack("<IHHHHIIH", 0x06054B50, 0, 0, len(entries), len(entries), len(central_blob), offset, 0)
    return b"".join(local) + central_blob + eocd


# ---------- Archiving one thread (fetch + build the ZIP; nothing is stored here) ----------

@dataclass
class ArchiveOutcome:
    ok: bool
    zip_bytes: Optional[bytes] = None
    asset_count: int = 0
    reason: Optional[str] = None
    entries: Optional[list] = None  # the ZipEntry list (index.html + assets/...), for bundle streaming
    blacklisted: bool = False       # skipped on purpose: the page text contains a BLACKLIST word
    replies_removed: int = 0        # replies left out because they contain a BLACKLIST_REPLY word


def archive_thread(board: Board, thread_id: str, cfg: Config, abort: http_util.Abort,
                   build: bool = True) -> ArchiveOutcome:
    page_url = f"{cfg.scheme}://{board.host}{board.path}res/{thread_id}.htm"
    res = http_util.request(page_url, headers={"User-Agent": cfg.user_agent}, timeout=30, abort=abort, accept_gzip=True)
    if not res.ok:
        return ArchiveOutcome(False, reason=f"thread fetch failed: {res.status}")
    raw_html = decode_buffer(res.body or b"", res.headers.get("content-type"))

    hit = blacklisted_word(raw_html, cfg.blacklist, cfg.blacklist_always)
    if hit:  # checked before any asset is downloaded, so a blocked thread costs one page fetch
        return ArchiveOutcome(False, reason=f'contains blacklisted word "{hit}"', blacklisted=True)

    hit = blacklisted_word(opening_post_html(raw_html), cfg.blacklist_reply)
    if hit:  # a BLACKLIST_REPLY word in the opening post: there is no thread without it, so skip the thread
        return ArchiveOutcome(False, reason=f'opening post contains blacklisted word "{hit}"', blacklisted=True)

    raw_html, replies_removed = strip_blacklisted_replies(raw_html, cfg.blacklist_reply)

    # Bare "<prefix>NNNN.ext" mentions too (including ones only in plain post text).
    offloaded = extract_offloaded_asset_urls(raw_html, cfg.offload_uploaders)

    html = rewrite_offloaded_references(raw_html, cfg.offload_uploaders)
    html = force_utf8_meta(html)

    # Thumbnails paired with a full-size <a href> are not fetched at all.
    thumb_to_full = extract_linked_thumbnails(html)
    stylesheet_urls = extract_stylesheet_urls(html)

    raw_refs = [u for u in extract_asset_urls(html) if u not in thumb_to_full]
    all_raw = list(dict.fromkeys(raw_refs + offloaded))

    refs = []
    for raw in all_raw[: cfg.max_assets_per_thread]:
        try:
            refs.append((raw, urljoin(page_url, raw)))
        except Exception:
            pass  # unparseable URL, skip

    fetched = fetch_assets(refs, cfg.user_agent, cfg.max_asset_bytes, cfg.asset_concurrency, abort)
    if abort.aborted:
        return ArchiveOutcome(False, reason="aborted (thread timeout)")

    used: set = set()
    zip_entries = []
    value_map = {}
    for a in fetched:
        local = local_filename_for(a.absolute, used)
        value_map[a.raw] = f"assets/{local}"
        zip_entries.append(
            ZipEntry(
                f"assets/{local}",
                a.data,
                compress=bool(re.search(r"\.(txt|html|mht)$", local, re.I)) or a.raw in stylesheet_urls,
            )
        )
    # Point each skipped thumbnail at the same local asset as its full-size image.
    for thumb, full in thumb_to_full.items():
        if full in value_map:
            value_map[thumb] = value_map[full]
    html = replace_attribute_values(html, value_map)

    zip_entries.insert(0, ZipEntry("index.html", html.encode("utf-8"), compress=True))
    return ArchiveOutcome(True, zip_bytes=build_zip(zip_entries) if build else None,
                          asset_count=len(fetched), entries=zip_entries, replies_removed=replies_removed)


# ---------- Spool + Internet Archive upload ----------

def board_slug(board: Board) -> str:
    return re.sub(r"[^a-z0-9]+", "_", f"{board.host}{board.path}", flags=re.I)


@dataclass
class RunCtx:
    state: State
    spool_bytes: int
    ia_down: bool = False
    ia_fail_streak: int = 0
    spool_full_logged: bool = False
    lock: threading.Lock = field(default_factory=threading.Lock)
    slowdown_streak: int = 0     # consecutive IA "SlowDown"/429 answers
    slowdown_count: int = 0      # SlowDown/429 answers on the current connection (not reset by successes)
    proxy: Optional[str] = None  # None = direct; else the proxy IA uploads currently go through
    proxy_idx: int = -1          # index into cfg.ia_proxies of the active proxy
    pace_s: float = 0.0          # current minimum gap between uploads (adaptive)
    next_upload_at: float = 0.0  # monotonic time before which no upload may start
    upload_sem: Optional[threading.Semaphore] = None
    fresh: set = field(default_factory=set)       # spool files built during this run
    claimed: set = field(default_factory=set)     # spool files the uploader already picked up this run
    fresh_uploaded: int = 0                       # of `fresh`, how many IA accepted
    drained: int = 0                              # older spool files IA accepted this run


def spool_path_for(cfg: Config, board: Board, thread_id: str, replies: int) -> str:
    return f"{cfg.spool_dir}/{board_slug(board)}__{thread_id}__{replies}.zip"


def write_file_atomic(path: str, data: bytes) -> None:
    tmp = f"{path}.part"
    with open(tmp, "wb") as f:
        f.write(data)
    os.replace(tmp, path)


def _remove_quietly(path: str) -> None:
    try:
        os.remove(path)
    except OSError:
        pass


# Wait between retries of one ZIP: IA_RETRY_BASE_MS x 1, 4, 12 (5s, 20s, 60s by default).
UPLOAD_BACKOFF_FACTORS = [1, 4, 12]


def _is_slowdown(r) -> bool:
    reason = r.reason or ""
    return r.overloaded or r.status == 429 or (
        r.status == 503
        and any(s in reason for s in ("SlowDown", "reduce your request rate", "bucket_tasks_queued"))
    )


def _switch_proxy_if_due(cfg: Config, ctx: RunCtx, used_proxy: Optional[str]) -> Optional[str]:
    """Call with ctx.lock held after a SlowDown. Counts it against the connection it
    came from and, once IA_PROXY_AFTER_SLOWDOWNS is reached and another proxy is
    left, moves every upload to that proxy with a clean slate. Returns the new proxy."""
    if used_proxy != ctx.proxy:  # answer to a request sent before the last switch
        return None
    ctx.slowdown_count += 1
    if ctx.slowdown_count < cfg.ia_proxy_after or ctx.proxy_idx + 1 >= len(cfg.ia_proxies):
        return None
    ctx.proxy_idx += 1
    ctx.proxy = cfg.ia_proxies[ctx.proxy_idx]
    ctx.slowdown_count = 0
    ctx.slowdown_streak = 0
    ctx.ia_fail_streak = 0
    ctx.pace_s = 0.0
    ctx.next_upload_at = 0.0
    return ctx.proxy


def _wait_for_slot(cfg: Config, ctx: RunCtx) -> None:
    """Pace uploads: no more than IA_UPLOAD_CONCURRENCY at once, and at least
    `ctx.pace_s` between the starts of two uploads (grows after every SlowDown)."""
    with ctx.lock:
        now = time.monotonic()
        start_at = max(now, ctx.next_upload_at)
        ctx.next_upload_at = start_at + max(ctx.pace_s, cfg.ia_min_interval_ms / 1000)
    delay = start_at - time.monotonic()
    if delay > 0:
        time.sleep(delay)


def _note_result(cfg: Config, ctx: RunCtx, r) -> float:
    """Adapt the pace to what IA just said. Returns extra seconds to cool down."""
    base = cfg.ia_min_interval_ms / 1000
    with ctx.lock:
        if r.ok:
            ctx.slowdown_streak = 0
            ctx.pace_s = max(base, ctx.pace_s * 0.8) if ctx.pace_s > base else base
            return 0.0
        if _is_slowdown(r):
            ctx.slowdown_streak += 1
            ctx.pace_s = min(60.0, max(base, ctx.pace_s, 1.0) * 2)
            if r.overloaded:
                return 0.0  # ia.py already waited for check_limit to clear
            cool = min(120.0, cfg.ia_retry_base_ms / 1000 * (2 ** (ctx.slowdown_streak - 1)))
            # push every other worker back too, so they don't all retry at once
            ctx.next_upload_at = max(ctx.next_upload_at, time.monotonic() + cool)
            return cool * random.uniform(1.0, 1.25)
    return 0.0


def try_upload(cfg: Config, ctx: RunCtx, row: SpoolRow) -> bool:
    """Upload one spooled ZIP. On success: record the thread as archived, drop the
    spool row and delete the file. On failure the ZIP stays in the spool.
      - the ZIP is streamed from its spool file (never held in memory).
      - before each PUT, IA's check_limit is asked whether it is rationing us (ia.py);
        if it still says yes after IA_OVERLOAD_WAIT_SECONDS the attempt counts as a SlowDown
        (but never switches proxy: the limit is per key/item, not per IP).
      - uploads are paced (IA_UPLOAD_CONCURRENCY, IA_MIN_INTERVAL_MS) and slow down
        further whenever IA answers SlowDown/429.
      - retryable errors (network, 429, 5xx) are retried here with backoff; three
        failures in a row (IA_SLOWDOWN_TRIP for SlowDown) pause all IA uploads for this run.
      - 401/403 pauses uploads immediately (bad credentials).
      - other 4xx are NOT retried now and count toward MAX_UPLOAD_ATTEMPTS."""
    board = Board(row.host, row.path)
    label = f"{row.host}{row.path}{row.thread_id}"
    if ctx.upload_sem is None:
        with ctx.lock:
            if ctx.upload_sem is None:
                ctx.upload_sem = threading.Semaphore(cfg.ia_upload_concurrency)

    attempt = 0
    while attempt < cfg.ia_attempts:
        attempt += 1
        if ctx.ia_down:
            return False
        with ctx.upload_sem:
            if ctx.ia_down:
                return False
            _wait_for_slot(cfg, ctx)
            used_proxy = ctx.proxy
            r = upload_to_ia(cfg.ia, board, row.thread_id, row.file, proxy=used_proxy)
            cooldown = _note_result(cfg, ctx, r)

        if r.ok:
            with ctx.lock:
                ctx.ia_fail_streak = 0
                ctx.spool_bytes = max(0, ctx.spool_bytes - row.size)
            ctx.state.record_archived(row.host, row.path, row.thread_id, row.replies, r.identifier, r.filename)
            ctx.state.remove_spool(row.file)
            _remove_quietly(row.file)
            log(f"{label} -> IA {r.identifier}/{r.filename} ({row.replies} replies, {fmt_bytes(row.size)})")
            return True

        reason = r.reason or "unknown error"
        ctx.state.note_spool_error(row.file, reason, not r.retryable and not r.fatal)
        log_error(f"{label} IA upload attempt {attempt}/{cfg.ia_attempts} failed: {reason}")

        if r.fatal:
            ctx.ia_down = True
            log_error("IA rejected our credentials (401/403) - pausing uploads for this run; ZIPs stay in the spool")
            return False
        if not r.retryable:
            return False

        slow = _is_slowdown(r)
        switched = None
        with ctx.lock:
            ctx.ia_fail_streak += 1
            if slow and not r.overloaded:
                switched = _switch_proxy_if_due(cfg, ctx, used_proxy)
            limit = cfg.ia_slowdown_trip if slow else 3
            tripped = ctx.ia_fail_streak >= limit
            if tripped:
                ctx.ia_down = True
        if switched and not ctx.ia_down:
            log(f"IA SlowDown x{cfg.ia_proxy_after} on {proxy_label(used_proxy)} - switching IA uploads to proxy "
                f"{proxy_label(switched)} ({ctx.proxy_idx + 1}/{len(cfg.ia_proxies)}) for the rest of this run")
            attempt -= 1  # the switch is not a failed attempt: retry right away on the new IP
            continue
        if tripped:
            log_error(f"IA failed {ctx.ia_fail_streak} times in a row - pausing uploads for this run; ZIPs stay in the spool")
            return False
        if r.overloaded:
            log(f"IA is still over its limit - skipped this upload, pace now {ctx.pace_s:.1f}s/upload")
        elif slow:
            log(f"IA says slow down - backing off {cooldown:.0f}s, pace now {ctx.pace_s:.1f}s/upload")
            time.sleep(cooldown)
        elif attempt < cfg.ia_attempts:
            factor = UPLOAD_BACKOFF_FACTORS[min(attempt - 1, len(UPLOAD_BACKOFF_FACTORS) - 1)]
            time.sleep(cfg.ia_retry_base_ms * factor / 1000)
    return False


def run_workers(n: int, fn) -> None:
    threads = [threading.Thread(target=fn, daemon=True) for _ in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()


def run_uploaders(cfg: Config, ctx: RunCtx, deadline: float, scraping_done: threading.Event) -> list:
    """Start the upload threads. They run WHILE boards are being scraped: each one
    takes the largest ZIP in the spool (UPLOAD_ORDER=oldest for oldest first) that it
    hasn't tried yet this run (backlog from earlier runs and ZIPs the download workers just finished), uploads it,
    and keeps going. They exit once `scraping_done` is set and nothing is left to
    try, or when IA is paused / the time budget is up."""

    largest_first = (_env("UPLOAD_ORDER") or "largest").lower() != "oldest"

    def worker():
        while True:
            if ctx.ia_down or time.time() >= deadline:
                return
            row = None
            with ctx.lock:
                rows = ctx.state.list_spool(1000, cfg.max_upload_attempts)  # oldest first
                if largest_first:
                    rows.sort(key=lambda r: r.size, reverse=True)  # stable: ties stay oldest first
                for r in rows:
                    if r.file not in ctx.claimed:
                        ctx.claimed.add(r.file)
                        row = r
                        break
            if row is None:
                if scraping_done.is_set():
                    return
                time.sleep(0.5)
                continue
            try:
                os.stat(row.file)
            except FileNotFoundError:
                log_error(f"spool file is gone, dropping its row (thread will be re-archived if still listed): {row.file}")
                ctx.state.remove_spool(row.file)
                with ctx.lock:
                    ctx.spool_bytes = max(0, ctx.spool_bytes - row.size)
                continue
            except OSError as e:
                log_error(f"cannot read spool file {row.file}: {e}")
                continue
            if try_upload(cfg, ctx, row):
                with ctx.lock:
                    if row.file in ctx.fresh:
                        ctx.fresh_uploaded += 1
                    else:
                        ctx.drained += 1

    threads = [threading.Thread(target=worker, daemon=True) for _ in range(max(1, cfg.ia_upload_concurrency))]
    for t in threads:
        t.start()
    return threads


# ---------- Deciding what to archive ----------

def needs_archive(prev: Optional[dict], replies: int, cfg: Config) -> bool:
    """Re-archive only on meaningful growth: a thread archived at P replies is archived
    again only once it reaches P + max(REARCHIVE_MIN_DELTA, P * REARCHIVE_GROWTH)."""
    if not prev:
        return True
    prev_replies = prev.get("replies") if isinstance(prev.get("replies"), (int, float)) else 0
    required = prev_replies + max(cfg.rearchive_min_delta, math.ceil(prev_replies * cfg.rearchive_growth))
    return replies >= required


def pick_threads(board: Board, entries: list, cfg: Config, state: State, limit: int) -> list:
    """Walk candidates in catalog order and take the first `limit` that need archiving.
    A thread that's built but still waiting in the spool counts as "already have it"."""
    picked = []
    for entry in entries:
        if len(picked) >= limit:
            break
        prev = state.get_archived(board.host, board.path, entry["id"]) or state.get_spooled(board.host, board.path, entry["id"])
        if needs_archive(prev, entry["replies"], cfg):
            picked.append(entry)
    return picked


# ---------- Running ----------

def archive_one(board: Board, pick: dict, cfg: Config, ctx: RunCtx) -> str:
    label = f"{board.host}{board.path}{pick['id']}"
    abort = http_util.Abort(cfg.thread_timeout_seconds, f"thread timeout after {cfg.thread_timeout_seconds}s")
    try:
        outcome = archive_thread(board, pick["id"], cfg, abort)
    except Exception as e:
        outcome = ArchiveOutcome(False, reason=str(e) or type(e).__name__)
    if not outcome.ok or not outcome.zip_bytes:
        log_error(f"{label} failed: {outcome.reason}")
        return "failed"

    # Save the finished ZIP to disk BEFORE uploading, so an IA outage (or a crash
    # mid-upload) never costs us the thread: it may be gone from the board by the
    # time we'd retry from scratch.
    data = outcome.zip_bytes
    file = spool_path_for(cfg, board, pick["id"], pick["replies"])
    row = SpoolRow(file, board.host, board.path, pick["id"], pick["replies"], len(data), now_ms(), 0, None)
    try:
        write_file_atomic(file, data)
        ctx.state.add_spool(row)
        with ctx.lock:
            ctx.spool_bytes += len(data)
    except Exception as e:
        log_error(f"{label} could not be saved to the spool (disk full?): {e}")
        _remove_quietly(f"{file}.part")
        return "failed"

    # Hand it to the uploader threads (run_uploaders) and go straight on to the next
    # thread: downloading and uploading happen at the same time.
    with ctx.lock:
        ctx.fresh.add(file)
    log(f"{label} built ({pick['replies']} replies, {outcome.asset_count} assets, {fmt_bytes(len(data))}) - queued for upload")
    return "built"


def process_board(board: Board, cfg: Config, ctx: RunCtx, limit: int, deadline: float) -> dict:
    label = f"{board.host}{board.path}"
    sort_param, sort_label = random.choice(SORT_CHOICES)
    summary = {
        "board": label, "sort": sort_label, "catalogEntries": 0, "candidates": 0,
        "picked": 0, "built": 0, "failed": 0,
    }
    try:
        entries = fetch_catalog_entries(board, sort_param, cfg)
    except Exception as e:
        summary["error"] = str(e) or type(e).__name__
        log_error(f"[{label}] {summary['error']}")
        return summary
    summary["catalogEntries"] = len(entries)

    candidates = [e for e in entries if e["replies"] >= cfg.min_replies][: cfg.max_candidates_per_board]
    summary["candidates"] = len(candidates)

    picked = pick_threads(board, candidates, cfg, ctx.state, min(cfg.max_threads_per_board, limit))
    summary["picked"] = len(picked)

    nxt = {"i": 0}
    lock = threading.Lock()

    def worker():
        while True:
            if time.time() >= deadline:
                return  # budget used up - the rest waits for the next run
            if ctx.spool_bytes >= cfg.spool_max_bytes:
                with ctx.lock:
                    first = not ctx.spool_full_logged
                    ctx.spool_full_logged = True
                if first:
                    log_error(f"spool is full ({fmt_bytes(ctx.spool_bytes)} >= SPOOL_MAX_GB) - not starting new threads until uploads catch up")
                if ctx.ia_down:
                    return  # uploads are paused, so the spool will not shrink
                time.sleep(1)  # uploader is working it down; wait for room
                continue
            with lock:
                i = nxt["i"]
                nxt["i"] += 1
            if i >= len(picked):
                return
            result = archive_one(board, picked[i], cfg, ctx)
            with lock:
                summary[result] += 1

    run_workers(min(cfg.thread_concurrency, len(picked)), worker)

    log(
        f"[{label}] sort={sort_label} catalog={len(entries)} candidates={len(candidates)} "
        f"picked={len(picked)} built={summary['built']} failed={summary['failed']}"
    )
    return summary


def order_boards(cfg: Config, cycle_index: int = 0) -> list:
    """Order: lead boards, then PRIORITY_BOARDS (in the order given), then every other
    board shuffled. The lead is the CYCLE_LEADS group whose turn it is; without
    CYCLE_LEADS there are no lead boards, so it's priority boards then the rest shuffled."""
    lead = list(cfg.cycle_groups[cycle_index % len(cfg.cycle_groups)]) if cfg.cycle_groups else []
    primary = [b for b in cfg.priority_boards if b not in lead]
    rest = [b for b in cfg.boards if b not in lead and b not in primary]
    random.shuffle(rest)
    return lead + primary + rest


def run(cfg: Config, state: State) -> dict:
    started = time.time()
    deadline = started + cfg.budget_seconds
    ctx = RunCtx(state=state, spool_bytes=state.spool_bytes())
    cycle_index = state.next_cycle() if cfg.cycle_groups else 0
    boards = order_boards(cfg, cycle_index)
    if cfg.cycle_groups:
        g = cfg.cycle_groups[cycle_index % len(cfg.cycle_groups)]
        lead = "leading with " + ", ".join(b.host + b.path for b in g) if g else "no lead boards, random order"
        log(f"cycle {(cycle_index % len(cfg.cycle_groups)) + 1}/{len(cfg.cycle_groups)}: {lead}")

    log(
        f"run starting - {len(boards)} board(s), budget={cfg.budget_seconds}s, maxThreadsPerRun={cfg.max_threads_per_run}, "
        f"maxThreadsPerBoard={cfg.max_threads_per_board}, concurrency={cfg.thread_concurrency}, ia={cfg.ia.endpoint}"
    )

    # Download and upload at the same time: the uploader threads start now and work
    # through the spool (old backlog first, then ZIPs as the download workers finish them).
    scraping_done = threading.Event()
    uploaders = run_uploaders(cfg, ctx, deadline, scraping_done)

    summaries = []
    remaining = cfg.max_threads_per_run
    built = failed = 0
    budget_exhausted = False

    for board in boards:
        if remaining <= 0:
            break
        if ctx.ia_down and ctx.spool_bytes >= cfg.spool_max_bytes:
            break
        if time.time() >= deadline:
            budget_exhausted = True
            break
        s = process_board(board, cfg, ctx, remaining, deadline)
        summaries.append(s)
        built += s["built"]
        failed += s["failed"]
        remaining -= s["built"] + s["failed"]
    if time.time() >= deadline:
        budget_exhausted = True

    # Boards done: let the uploaders finish whatever is still queued, then stop.
    scraping_done.set()
    for t in uploaders:
        t.join()

    archived = ctx.fresh_uploaded
    spooled = built - archived
    drained = {"uploaded": ctx.drained}

    # Healthy unless IA is refusing uploads, or we tried to archive things and none
    # got through, or no board's catalog would even load.
    attempted = archived + spooled + failed
    all_catalogs_failed = bool(summaries) and all("error" in s for s in summaries)
    nothing_got_through = attempted > 0 and archived == 0 and drained["uploaded"] == 0
    ok = not ctx.ia_down and not nothing_got_through and not all_catalogs_failed

    return {
        "ok": ok,
        "boardsConfigured": len(cfg.boards),
        "boardsVisited": len(summaries),
        "drainedFromSpool": drained["uploaded"],
        "archived": archived,
        "spooled": spooled,
        "failed": failed,
        "iaPaused": ctx.ia_down,
        "elapsedSeconds": round((time.time() - started) * 10) / 10,
        "budgetExhausted": budget_exhausted,
        "boards": summaries,
    }


# ---------- Entry point ----------

def main() -> int:
    args = sys.argv[1:]
    status_only = "--status" in args
    try:
        cfg = load_config(require_ia=not status_only)
    except Exception as e:
        log_error(f"configuration error: {e}")
        return 1

    os.makedirs(cfg.spool_dir, exist_ok=True)
    state = State(cfg.db_path)

    if status_only:
        st = state.stats(cfg.max_upload_attempts)
        print(json.dumps(st, indent=2))
        state.close()
        return 0

    # Safety net: if something hangs in a way the per-request timeouts don't catch,
    # never let one run outlive its slot (and block the next one).
    def hard_exit():
        log_error(f"hard deadline hit ({cfg.budget_seconds}s budget + {cfg.hard_grace_seconds}s grace) - exiting")
        os._exit(2)

    watchdog = threading.Timer(cfg.budget_seconds + cfg.hard_grace_seconds, hard_exit)
    watchdog.daemon = True
    watchdog.start()

    summary = run(cfg, state)
    stats = state.stats(cfg.max_upload_attempts)
    log(
        f"run complete in {summary['elapsedSeconds']}s - visited {summary['boardsVisited']}/{summary['boardsConfigured']} board(s), "
        f"uploaded {summary['archived']}, spooled {summary['spooled']}, failed {summary['failed']}, drained {summary['drainedFromSpool']}"
        f"{' (IA paused)' if summary['iaPaused'] else ''}{' (time budget used up)' if summary['budgetExhausted'] else ''}"
    )
    log(
        f"state: {stats['archivedTotal']} archived total ({stats['archivedLast24h']} in last 24h), "
        f"spool {stats['spoolFiles']} file(s) / {fmt_bytes(stats['spoolBytes'])}"
        f"{', ' + str(stats['spoolStuck']) + ' STUCK' if stats['spoolStuck'] else ''}"
    )
    print(json.dumps({**summary, "state": stats}, indent=2), flush=True)
    state.close()
    return 0 if summary["ok"] else 1


if __name__ == "__main__":
    try:
        code = main()
    except Exception as e:  # noqa: BLE001
        log_error(f"unhandled error in main(): {e}")
        code = 1
    sys.stdout.flush()
    sys.exit(code)