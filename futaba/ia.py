"""
Internet Archive (IAS3) uploader, built on the official `internetarchive` library
(https://internetarchive.readthedocs.io).

What the library does for us:
  - signs and sends the S3-style PUT (auth, x-archive-meta-* headers, size hint, queue-derive)
  - ArchiveSession.s3_is_overloaded() asks IA's `s3.us.archive.org?check_limit=1` whether this
    access key / item is currently being rationed. We call it before every upload and wait
    for it to clear instead of sending a PUT that IA would answer with 503 SlowDown.
  - optional Content-MD5 (IA_VERIFY=on): IA rejects the upload if the bytes it received differ.

What stays ours (archiver.py): pacing, SlowDown backoff, proxy fallback, the spool, "stuck" handling.
The library's own 503 retry loop is switched off (retries=0) so those rules stay in one place.

Items can be sharded by month (IA_ITEM_SHARD=month, the default) so one busy board doesn't grow a
single item without bound. IA_ITEM_SHARD=none keeps the old one-item-per-board names. Failures are
classified so the caller knows whether to retry.
"""
import io
import os
import re
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Optional, Union

import requests
from internetarchive import Item, get_session
from requests.adapters import HTTPAdapter

DEFAULT_ENDPOINT = "https://s3.us.archive.org"  # hard-coded inside the library


@dataclass
class IaConfig:
    access_key: str
    secret_key: str
    endpoint: str  # no trailing slash; anything but DEFAULT_ENDPOINT is for tests / mirrors
    collection: str
    mediatype: str
    item_prefix: str
    shard: str  # "month" | "none"
    timeout_s: float
    identifier: Optional[str] = None  # if set, EVERY upload goes into this one item
    title: Optional[str] = None
    description: Optional[str] = None
    check_limit: bool = True          # ask IA (check_limit) before each upload
    overload_wait_s: float = 45.0     # how long one upload waits for the limit to clear
    overload_poll_s: float = 15.0     # gap between check_limit polls while waiting
    verify_md5: bool = True           # send Content-MD5 so IA verifies what it received
    log: Optional[Callable[[str], None]] = None


@dataclass
class IaResult:
    ok: bool
    retryable: bool = False  # worth trying again later: network errors, 408/429, 5xx
    fatal: bool = False  # credentials/permission problem: stop uploading this run
    status: Optional[int] = None
    reason: Optional[str] = None
    identifier: Optional[str] = None
    filename: Optional[str] = None
    overloaded: bool = False  # check_limit said IA is rationing us; no PUT was sent


def sanitize_ia_identifier_part(s: str) -> str:
    s = s.lower()
    s = re.sub(r"[^a-z0-9_.-]+", "-", s)
    s = re.sub(r"-+", "-", s)
    return re.sub(r"^[-.]+|[-.]+$", "", s)


def month_stamp(dt: Optional[datetime] = None) -> str:
    dt = dt or datetime.now(timezone.utc)
    return f"{dt.year}-{dt.month:02d}"


def ia_item_identifier(cfg: IaConfig, board, dt: Optional[datetime] = None) -> str:
    configured = cfg.identifier and sanitize_ia_identifier_part(cfg.identifier)
    if configured:
        return re.sub(r"-+$", "", configured[:100])
    slug = sanitize_ia_identifier_part(f"{board.host}{board.path}")
    suffix = f"-{month_stamp(dt)}" if cfg.shard == "month" else ""
    room = 100 - len(suffix)
    return re.sub(r"-+$", "", f"{cfg.item_prefix}-{slug}"[:room]) + suffix


def ia_filename(board, thread_id: str) -> str:
    return f"{sanitize_ia_identifier_part(f'{board.host}{board.path}')}_{thread_id}.zip"


# ---------- library sessions ----------

class _EndpointAdapter(HTTPAdapter):
    """The library always talks to https://s3.us.archive.org. When IA_ENDPOINT points somewhere
    else (the offline tests, a mirror), re-aim those requests there. Auth is a plain
    `LOW key:secret` header, so nothing is signed over the URL."""

    def __init__(self, endpoint: str):
        super().__init__()
        self._endpoint = endpoint.rstrip("/")

    def send(self, request, **kwargs):
        if request.url.startswith(DEFAULT_ENDPOINT):
            request.url = self._endpoint + request.url[len(DEFAULT_ENDPOINT):]
        return super().send(request, **kwargs)


_sessions: dict = {}
_sessions_lock = threading.Lock()


def _session_for(cfg: IaConfig, proxy: Optional[str]):
    """One ArchiveSession per (credentials, endpoint, proxy, thread): requests sessions are not
    meant to be shared between threads. With a proxy, EVERYTHING on that session (the upload and
    the check_limit call) goes through it."""
    key = (cfg.access_key, cfg.endpoint, proxy, threading.get_ident())
    with _sessions_lock:
        s = _sessions.get(key)
        if s is None:
            s = get_session(config={"s3": {"access": cfg.access_key, "secret": cfg.secret_key}})
            if proxy:
                s.proxies.update({"http": proxy, "https": proxy})
            if cfg.endpoint.rstrip("/") != DEFAULT_ENDPOINT:
                s.mount(DEFAULT_ENDPOINT, _EndpointAdapter(cfg.endpoint))
            _sessions[key] = s
        return s


def _still_overloaded(cfg: IaConfig, session, identifier: str) -> bool:
    """Ask IA (check_limit) whether it is rationing this access key / item, and wait up to
    overload_wait_s for it to clear. True = still overloaded when the wait ran out.
    If the check itself fails (network, odd answer) we can't tell, so we just try the upload."""
    if not cfg.check_limit:
        return False
    deadline = time.monotonic() + cfg.overload_wait_s
    announced = False
    while True:
        try:
            over = session.s3_is_overloaded(identifier=identifier, access_key=cfg.access_key)
        except Exception:  # noqa: BLE001
            return False
        if not over:
            return False
        if time.monotonic() + cfg.overload_poll_s > deadline:
            return True
        if not announced and cfg.log:
            cfg.log(f"IA reports S3 is over its limit (check_limit) - holding uploads up to {cfg.overload_wait_s:.0f}s")
            announced = True
        time.sleep(cfg.overload_poll_s)


# ---------- upload ----------

def upload_to_ia(cfg: IaConfig, board, thread_id: str, source: Union[str, bytes],
                 proxy: Optional[str] = None) -> IaResult:
    """Upload one thread ZIP. `source` is a file path (streamed from disk) or the ZIP's bytes."""
    identifier = ia_item_identifier(cfg, board)
    filename = ia_filename(board, thread_id)
    folder = f"{board.host}{board.path}"
    month = month_stamp()

    title = cfg.title or f"Futaba thread archive ({folder})" + (f" {month}" if cfg.shard == "month" else "")
    description = cfg.description or (
        f"Archived Futaba Channel threads from {folder}, each saved as a ZIP "
        "(index.html + assets/). Uploaded automatically by futaba-archiver."
    )
    metadata = {
        "collection": cfg.collection,
        "mediatype": cfg.mediatype,
        "title": title,
        "description": description,
        "subject": "futaba;imageboard;archive",
    }
    headers = {
        "x-archive-ignore-preexisting-bucket": "1",
        "Content-Type": "application/zip",
    }

    session = _session_for(cfg, proxy)
    if _still_overloaded(cfg, session, identifier):
        return IaResult(
            False, retryable=True, status=503, overloaded=True, identifier=identifier, filename=filename,
            reason=f"IA S3 still over its limit after {cfg.overload_wait_s:.0f}s (check_limit); no upload sent",
        )

    try:
        body = io.BytesIO(source) if isinstance(source, (bytes, bytearray)) else open(source, "rb")
    except OSError as e:
        return IaResult(False, reason=f"cannot read ZIP to upload: {e}", identifier=identifier, filename=filename)

    try:
        # Item(...) directly, not get_item(): get_item() would first fetch the item's metadata
        # (an extra request per upload) and we don't need it to PUT a file.
        item = Item(session, identifier)
        resp = item.upload_file(
            body,                      # the library closes it
            key=filename,
            metadata=metadata,
            headers=headers,
            access_key=cfg.access_key,
            secret_key=cfg.secret_key,
            queue_derive=False,
            verify=cfg.verify_md5,
            retries=0,                 # SlowDown handling lives in archiver.py
            request_kwargs={"timeout": cfg.timeout_s},
        )
        return IaResult(True, status=getattr(resp, "status_code", 200), identifier=identifier, filename=filename)
    except requests.exceptions.HTTPError as e:
        resp = e.response
        status = resp.status_code if resp is not None else None
        try:
            text = (resp.text or "")[:300] if resp is not None else ""
        except Exception:  # noqa: BLE001
            text = ""
        return IaResult(
            False,
            status=status,
            retryable=status is None or status in (408, 429) or status >= 500,
            fatal=status in (401, 403),
            reason=f"IA {status}: {text or e}",
            identifier=identifier,
            filename=filename,
        )
    except Exception as e:  # noqa: BLE001  (timeouts, resets, DNS, ...)
        return IaResult(False, retryable=True, reason=f"IA upload request failed: {e}",
                        identifier=identifier, filename=filename)


# ---------- bundle upload (one big zip + manifest into one item) ----------

def _fmt_bytes(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1000 or unit == "TB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1000


def _fmt_eta(seconds: float) -> str:
    seconds = int(max(0, seconds))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    return f"{h}h{m:02d}m" if h else f"{m}m{s:02d}s"


class _ProgressFile:
    """Wraps the open file the IA library uploads. The library first reads the whole file once to
    compute its MD5 (verify=True), seeks back to 0, then streams it to the server; progress is the
    file position, which covers both phases. Everything else is delegated to the real file."""

    def __init__(self, f, size: int, hashing_first: bool):
        self._f = f
        self.size = size
        self.phase = "hashing" if hashing_first else "uploading"
        self._at_end = False
        self.upload_started = time.monotonic()

    def read(self, *args):
        data = self._f.read(*args)
        if not data or self._f.tell() >= self.size:
            self._at_end = True
        return data

    def seek(self, offset, whence=0):
        if self.phase == "hashing" and self._at_end and offset == 0 and whence == 0:
            self.phase = "uploading"       # MD5 pass finished, the real upload starts now
            self.upload_started = time.monotonic()
        return self._f.seek(offset, whence)

    def position(self) -> int:
        try:
            return self._f.tell()
        except (ValueError, OSError):      # file closed
            return self.size

    def __getattr__(self, name):
        return getattr(self._f, name)


def _report_progress(pf: _ProgressFile, name: str, every_s: float, stop, log) -> None:
    last_pos, last_t, last_phase = 0, time.monotonic(), pf.phase
    while not stop.wait(every_s):
        phase, pos, now = pf.phase, pf.position(), time.monotonic()
        if phase != last_phase:                    # switching from hashing to uploading
            last_pos, last_t, last_phase = 0, pf.upload_started, phase
        pct = 100.0 * pos / pf.size if pf.size else 100.0
        speed = (pos - last_pos) / max(1e-6, now - last_t)
        last_pos, last_t = pos, now
        if phase == "hashing":
            log(f"  {name}: checking MD5 before upload {pct:.0f}% ({_fmt_bytes(pos)} / {_fmt_bytes(pf.size)})")
        elif pos >= pf.size:
            log(f"  {name}: all {_fmt_bytes(pf.size)} sent, waiting for IA to confirm")
        else:
            eta = f", about {_fmt_eta((pf.size - pos) / speed)} left" if speed > 0 else ""
            log(f"  {name}: uploading {pct:.0f}% ({_fmt_bytes(pos)} / {_fmt_bytes(pf.size)}) "
                f"at {_fmt_bytes(speed)}/s{eta}")


def upload_bundle_file(cfg: IaConfig, identifier: str, path: str, filename: str, metadata: dict,
                       proxy: Optional[str] = None, progress_s: float = 15.0) -> IaResult:
    """Upload ONE file (the big bundle zip or its manifest) into `identifier`, streamed from disk,
    logging a progress line every `progress_s` seconds (0 = off).
    Same classification of failures as upload_to_ia: retryable / fatal / permanent."""
    session = _session_for(cfg, proxy)
    if _still_overloaded(cfg, session, identifier):
        return IaResult(False, retryable=True, status=503, overloaded=True, identifier=identifier,
                        filename=filename,
                        reason=f"IA S3 still over its limit after {cfg.overload_wait_s:.0f}s (check_limit); no upload sent")
    try:
        raw = open(path, "rb")
        size = os.path.getsize(path)
    except OSError as e:
        return IaResult(False, reason=f"cannot read {path}: {e}", identifier=identifier, filename=filename)
    body = _ProgressFile(raw, size, hashing_first=bool(cfg.verify_md5))
    ctype = "application/zip" if filename.endswith(".zip") else "text/plain; charset=utf-8"
    stop = threading.Event()
    reporter = None
    if progress_s and progress_s > 0 and size > 0:
        reporter = threading.Thread(target=_report_progress, args=(body, filename, progress_s, stop, cfg.log),
                                    daemon=True)
        reporter.start()
    try:
        item = Item(session, identifier)
        resp = item.upload_file(
            body, key=filename, metadata=metadata,
            headers={"x-archive-ignore-preexisting-bucket": "1", "Content-Type": ctype},
            access_key=cfg.access_key, secret_key=cfg.secret_key,
            queue_derive=False, verify=cfg.verify_md5, retries=0,
            request_kwargs={"timeout": cfg.timeout_s},
        )
        return IaResult(True, status=getattr(resp, "status_code", 200), identifier=identifier, filename=filename)
    except requests.exceptions.HTTPError as e:
        resp = e.response
        status = resp.status_code if resp is not None else None
        try:
            text = (resp.text or "")[:300] if resp is not None else ""
        except Exception:  # noqa: BLE001
            text = ""
        return IaResult(False, status=status,
                        retryable=status is None or status in (408, 429) or status >= 500,
                        fatal=status in (401, 403), reason=f"IA {status}: {text or e}",
                        identifier=identifier, filename=filename)
    except Exception as e:  # noqa: BLE001
        return IaResult(False, retryable=True, reason=f"IA upload request failed: {e}",
                        identifier=identifier, filename=filename)
    finally:
        stop.set()
