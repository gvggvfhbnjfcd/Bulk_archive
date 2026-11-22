"""
Small HTTP helper on top of urllib (standard library only).

- A total time limit per request that also covers reading the body, plus an
  optional shared Abort (a thread-wide deadline) that cancels every request of a
  thread at once.
- Non-2xx answers come back as a normal Response (no exception).
- Non-ASCII / space characters in URLs are percent-encoded, like a browser would.
- 307/308 redirects keep the method and body (needed for IA uploads); the
  Authorization header is dropped when a redirect leaves the original host.
"""
import gzip
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from typing import Optional


class Abort:
    """A deadline shared by everything belonging to one thread-archive job."""

    def __init__(self, seconds: float, reason: str = "timeout"):
        self.deadline = time.monotonic() + seconds
        self.reason = reason

    @property
    def aborted(self) -> bool:
        return time.monotonic() >= self.deadline

    def remaining(self) -> float:
        return self.deadline - time.monotonic()


@dataclass
class Response:
    status: int
    headers: dict = field(default_factory=dict)  # lower-case names
    body: Optional[bytes] = None  # None when not ok or skipped
    skipped: bool = False  # skipped because of max_bytes

    @property
    def ok(self) -> bool:
        return 200 <= self.status < 300


def normalize_url(url: str) -> str:
    p = urllib.parse.urlsplit(url)
    safe = "%/:@!$&'()*+,;=~-._"
    path = urllib.parse.quote(p.path, safe=safe)
    query = urllib.parse.quote(p.query, safe=safe + "?")
    host = p.hostname or ""
    try:
        host.encode("ascii")
    except UnicodeEncodeError:
        host = host.encode("idna").decode("ascii")
    netloc = host
    if ":" in host:  # ipv6 literal
        netloc = f"[{host}]"
    if p.port:
        netloc += f":{p.port}"
    if p.username:
        netloc = f"{p.username}{':' + p.password if p.password else ''}@{netloc}"
    return urllib.parse.urlunsplit((p.scheme, netloc, path, query, ""))


class _KeepMethodRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        if code in (307, 308):
            hdrs = {k: v for k, v in req.header_items()}
            if urllib.parse.urlsplit(newurl).netloc != urllib.parse.urlsplit(req.full_url).netloc:
                for k in list(hdrs):
                    if k.lower() == "authorization":
                        del hdrs[k]
            return urllib.request.Request(
                newurl, data=req.data, headers=hdrs, method=req.get_method(), unverifiable=True
            )
        return super().redirect_request(req, fp, code, msg, headers, newurl)


_opener = urllib.request.build_opener(_KeepMethodRedirect())
_proxy_openers: dict = {}


def _opener_for(proxy: Optional[str]):
    """Default opener (honours the http(s)_proxy environment), or one that sends
    everything through `proxy` (e.g. http://user:pass@host:port)."""
    if not proxy:
        return _opener
    op = _proxy_openers.get(proxy)
    if op is None:
        op = urllib.request.build_opener(
            urllib.request.ProxyHandler({"http": proxy, "https": proxy}), _KeepMethodRedirect()
        )
        _proxy_openers[proxy] = op
    return op


def _lower(headers) -> dict:
    return {k.lower(): v for k, v in headers.items()} if headers is not None else {}


def request(
    url: str,
    method: str = "GET",
    headers: Optional[dict] = None,
    data: Optional[bytes] = None,
    timeout: float = 30,
    abort: Optional[Abort] = None,
    max_bytes: int = 0,
    accept_gzip: bool = False,
    read_body: bool = True,
    proxy: Optional[str] = None,
) -> Response:
    """Raises OSError / TimeoutError on network failure or when time runs out."""
    hdrs = dict(headers or {})
    if accept_gzip:
        hdrs["Accept-Encoding"] = "gzip"
    deadline = time.monotonic() + timeout
    if abort is not None:
        deadline = min(deadline, abort.deadline)

    def remaining() -> float:
        r = deadline - time.monotonic()
        if r <= 0:
            raise TimeoutError("request timed out" if abort is None or not abort.aborted else abort.reason)
        return r

    req = urllib.request.Request(normalize_url(url), data=data, headers=hdrs, method=method)
    try:
        resp = _opener_for(proxy).open(req, timeout=remaining())
    except urllib.error.HTTPError as e:
        try:
            body = e.read(2000) if read_body else None
        except Exception:
            body = None
        finally:
            e.close()
        return Response(e.code, _lower(e.headers), body=None if body is None else body)
    # urlopen's own timeout is per socket operation, so the total is enforced between chunks
    try:
        h = _lower(resp.headers)
        if max_bytes:
            try:
                if int(h.get("content-length", "")) > max_bytes:
                    return Response(resp.status, h, None, skipped=True)
            except ValueError:
                pass
        if not read_body:
            return Response(resp.status, h, None)
        chunks = []
        total = 0
        while True:
            chunk = resp.read(65536)
            if not chunk:
                break
            total += len(chunk)
            if max_bytes and total > max_bytes:
                return Response(resp.status, h, None, skipped=True)
            chunks.append(chunk)
            remaining()
        body = b"".join(chunks)
        if h.get("content-encoding", "").lower() == "gzip":
            body = gzip.decompress(body)
        return Response(resp.status, h, body)
    finally:
        resp.close()
