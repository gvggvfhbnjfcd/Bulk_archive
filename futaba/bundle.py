"""
Streaming "one big zip" bundle.

Threads are appended to ONE growing file (current/bundle.zip.part) the moment
they are built; nothing is spooled per thread. A ZIP's central directory has to
sit at the END of the file, so while the bundle is open we keep a small JSONL
index next to it instead, and only write the real central directory when the
bundle is sealed.

Crash safety: a thread's entries are written to the data file, fsynced, and only
then are its index lines + a "commit" line appended. On startup, recover()
throws away anything after the last commit, so a killed run can never leave a
half-written thread or a corrupt bundle behind.

Layout on disk (DATA_DIR):
    current/            the bundle being filled
        meta.json         {"id": "20261120T031500Z"}
        bundle.zip.part   entry data (no central directory yet)
        index.jsonl       one line per entry + one "commit" line per thread
    ready/<id>/         sealed bundles waiting for upload (atomic dir rename)
        bundle.zip        complete, valid ZIP (zip64 when needed)
        manifest.txt      host/path <TAB> thread_id <TAB> replies
        SEALED            marker, written before the rename
"""
import json
import os
import struct
import zlib
from datetime import datetime, timezone

# Module-level so tests can lower them to exercise the zip64 paths.
LIMIT32 = 0xFFFFFFFF
LIMIT16 = 0xFFFF

DATA_NAME = "bundle.zip.part"
INDEX_NAME = "index.jsonl"
META_NAME = "meta.json"


def _dos_now():
    d = datetime.now(timezone.utc)
    t = (d.hour << 11) | (d.minute << 5) | (d.second >> 1)
    dt = ((max(1980, d.year) - 1980) << 9) | (d.month << 5) | d.day
    return t, dt


def _deflate_raw(data: bytes) -> bytes:
    c = zlib.compressobj(6, zlib.DEFLATED, -15)
    return c.compress(data) + c.flush()


def new_bundle_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _fsync_dir(path: str) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except OSError:
        pass


class Bundle:
    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        self.dir = os.path.join(data_dir, "current")
        self.ready_dir = os.path.join(data_dir, "ready")
        self.data_path = os.path.join(self.dir, DATA_NAME)
        self.index_path = os.path.join(self.dir, INDEX_NAME)
        self.meta_path = os.path.join(self.dir, META_NAME)
        os.makedirs(self.ready_dir, exist_ok=True)
        self.id = None
        self.records = []   # committed entry records
        self.threads = []   # committed (host_path, thread_id, replies)
        self._size = 0

    # ---------- open / recover ----------

    def open(self) -> None:
        """Create the bundle if there is none, otherwise recover it."""
        if os.path.exists(os.path.join(self.dir, "bundle.zip")) and os.path.exists(self.meta_path):
            # crashed during seal(): the central directory and manifest were already written
            sealed = os.path.join(self.dir, "SEALED")
            if not os.path.exists(sealed):
                with open(self.meta_path) as f:
                    bid = json.load(f)["id"]
                with open(sealed, "w") as f:
                    f.write(bid + "\n")
            self._finish_rename()  # crashed between marker and rename
        os.makedirs(self.dir, exist_ok=True)
        if os.path.exists(self.meta_path):
            with open(self.meta_path) as f:
                self.id = json.load(f)["id"]
        else:
            self.id = new_bundle_id()
            with open(self.meta_path, "w") as f:
                json.dump({"id": self.id}, f)
        self._recover()

    def _recover(self) -> None:
        records, threads, pending = [], [], []
        good_idx_end = good_data_end = 0
        if os.path.exists(self.index_path):
            with open(self.index_path, "rb") as f:
                pos = 0
                for raw in f:
                    pos += len(raw)
                    try:
                        rec = json.loads(raw)
                    except ValueError:
                        break  # torn last line
                    if "commit" in rec:
                        records += pending
                        pending = []
                        threads.append((rec["hp"], rec["id"], rec["replies"]))
                        good_idx_end, good_data_end = pos, rec["end"]
                    else:
                        pending.append(rec)
        # drop everything after the last commit
        with open(self.index_path, "ab") as f:
            f.truncate(good_idx_end)
        with open(self.data_path, "ab") as f:
            f.truncate(good_data_end)
        self.records, self.threads, self._size = records, threads, good_data_end

    # ---------- state ----------

    @property
    def size(self) -> int:
        return self._size

    @property
    def thread_count(self) -> int:
        return len(self.threads)

    # ---------- writing ----------

    def add_thread(self, host_path: str, thread_id: str, replies: int, prefix: str, entries) -> int:
        """Append all of one thread's entries (ZipEntry objects, names relative to the thread)
        under `prefix`, then commit. Returns bytes added. Call under a lock."""
        offset = self._size
        recs = []
        t, d = _dos_now()
        with open(self.data_path, "r+b") as f:
            f.seek(offset)
            for e in entries:
                name = (prefix + e.name).encode("utf-8")
                data = e.data
                crc = zlib.crc32(data) & 0xFFFFFFFF
                method, stored = 0, data
                if e.compress:
                    comp = _deflate_raw(data)
                    if len(comp) < len(data):
                        method, stored = 8, comp
                if len(stored) >= LIMIT32 or len(name) > 0xFFFF:
                    raise ValueError(f"entry too large or name too long: {prefix + e.name}")
                hdr = struct.pack("<IHHHHHIIIHH", 0x04034B50, 20, 0x0800, method, t, d,
                                  crc, len(stored), len(data), len(name), 0)
                f.write(hdr)
                f.write(name)
                f.write(stored)
                recs.append({"n": prefix + e.name, "o": offset, "m": method, "c": crc,
                             "cs": len(stored), "us": len(data), "t": t, "d": d})
                offset += len(hdr) + len(name) + len(stored)
            f.flush()
            os.fsync(f.fileno())
        lines = "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in recs)
        lines += json.dumps({"commit": 1, "hp": host_path, "id": thread_id,
                             "replies": replies, "end": offset}) + "\n"
        with open(self.index_path, "ab") as f:
            f.write(lines.encode("utf-8"))
            f.flush()
            os.fsync(f.fileno())
        added = offset - self._size
        self.records += recs
        self.threads.append((host_path, thread_id, replies))
        self._size = offset
        return added

    # ---------- sealing ----------

    def seal(self) -> str:
        """Write the central directory, a manifest and the SEALED marker, then move the whole
        directory to ready/<id> in one atomic rename. Returns the new path (or "" if empty)."""
        if not self.threads:
            return ""
        # dedupe names (a crash between commit and DB write can cause a re-archive): keep the last
        last = {}
        for r in self.records:
            last[r["n"]] = r
        recs = list(last.values())

        with open(self.data_path, "r+b") as f:
            f.seek(self._size)
            cd_start = self._size
            cd = bytearray()
            for r in recs:
                name = r["n"].encode("utf-8")
                extra = b""
                off32 = r["o"]
                ver = 20
                if r["o"] >= LIMIT32:
                    extra = struct.pack("<HHQ", 0x0001, 8, r["o"])
                    off32 = 0xFFFFFFFF
                    ver = 45
                cd += struct.pack("<IHHHHHHIIIHHHHHII", 0x02014B50, ver, ver, 0x0800, r["m"], r["t"], r["d"],
                                  r["c"], r["cs"], r["us"], len(name), len(extra), 0, 0, 0, 0, off32)
                cd += name + extra
            f.write(cd)
            cd_size = len(cd)
            n = len(recs)
            if n >= LIMIT16 or cd_start >= LIMIT32 or cd_size >= LIMIT32:
                z64_off = cd_start + cd_size
                f.write(struct.pack("<IQHHIIQQQQ", 0x06064B50, 44, 45, 45, 0, 0, n, n, cd_size, cd_start))
                f.write(struct.pack("<IIQI", 0x07064B50, 0, z64_off, 1))
                f.write(struct.pack("<IHHHHIIH", 0x06054B50, 0, 0, min(n, 0xFFFF), min(n, 0xFFFF),
                                    min(cd_size, 0xFFFFFFFF), min(cd_start, 0xFFFFFFFF), 0))
            else:
                f.write(struct.pack("<IHHHHIIH", 0x06054B50, 0, 0, n, n, cd_size, cd_start, 0))
            f.flush()
            os.fsync(f.fileno())

        with open(os.path.join(self.dir, "manifest.txt"), "w", encoding="utf-8") as f:
            for hp, tid, replies in self.threads:
                f.write(f"{hp}\t{tid}\t{replies}\n")
        os.replace(self.data_path, os.path.join(self.dir, "bundle.zip"))
        os.remove(self.index_path)
        with open(os.path.join(self.dir, "SEALED"), "w") as f:
            f.write(self.id + "\n")
            f.flush()
            os.fsync(f.fileno())
        dest = self._finish_rename()
        self.records, self.threads, self._size, self.id = [], [], 0, None
        return dest

    def _finish_rename(self) -> str:
        with open(os.path.join(self.dir, "SEALED")) as f:
            bid = f.read().strip()
        dest = os.path.join(self.ready_dir, bid)
        os.rename(self.dir, dest)
        _fsync_dir(self.ready_dir)
        return dest


def list_ready(data_dir: str) -> list:
    """Sealed bundle directories, oldest first."""
    root = os.path.join(data_dir, "ready")
    if not os.path.isdir(root):
        return []
    out = []
    for name in sorted(os.listdir(root)):
        p = os.path.join(root, name)
        if os.path.exists(os.path.join(p, "SEALED")) and os.path.exists(os.path.join(p, "bundle.zip")):
            out.append(p)
    return out


def file_stem(bundle_id: str) -> str:
    """Name of a bundle's files inside its IA item: futaba_<id>.zip and futaba_<id>_manifest.txt"""
    return f"futaba_{bundle_id}"
