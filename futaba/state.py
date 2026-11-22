"""
Local state for the archiver (SQLite, one file, no network).

  - archived: one row per archived thread
  - spool:    zips that are built and saved on local disk but not yet uploaded
              to Internet Archive. A row is deleted only after IA accepted the
              upload.
  - meta:     small counters (run-cycle index)

The schema is identical to the Deno/TypeScript version, so an existing
state.db can be copied over and used as-is.
"""
import sqlite3
import threading
import time
from dataclasses import dataclass
from typing import Optional


@dataclass
class SpoolRow:
    file: str
    host: str
    path: str
    thread_id: str
    replies: int
    size: int
    created_at: int
    attempts: int = 0
    last_error: Optional[str] = None


def now_ms() -> int:
    return int(time.time() * 1000)


class State:
    def __init__(self, path: str):
        # autocommit; one connection shared by worker threads, serialised by a lock
        self.db = sqlite3.connect(path, isolation_level=None, check_same_thread=False, timeout=10)
        self.lock = threading.RLock()
        self.db.execute("PRAGMA journal_mode = WAL")
        self.db.execute("PRAGMA synchronous = NORMAL")
        self.db.execute("PRAGMA busy_timeout = 10000")
        self.db.executescript(
            """
            CREATE TABLE IF NOT EXISTS archived (
              host        TEXT    NOT NULL,
              path        TEXT    NOT NULL,
              thread_id   TEXT    NOT NULL,
              replies     INTEGER NOT NULL,
              archived_at INTEGER NOT NULL,
              ia_item     TEXT,
              ia_file     TEXT,
              PRIMARY KEY (host, path, thread_id)
            );
            CREATE TABLE IF NOT EXISTS spool (
              file       TEXT PRIMARY KEY,
              host       TEXT    NOT NULL,
              path       TEXT    NOT NULL,
              thread_id  TEXT    NOT NULL,
              replies    INTEGER NOT NULL,
              size       INTEGER NOT NULL,
              created_at INTEGER NOT NULL,
              attempts   INTEGER NOT NULL DEFAULT 0,
              last_error TEXT
            );
            CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS spool_thread ON spool (host, path, thread_id);
            """
        )

    def close(self) -> None:
        with self.lock:
            self.db.close()

    def _one(self, sql, *params):
        return self.db.execute(sql, params).fetchone()

    def next_cycle(self) -> int:
        """Returns the run-cycle counter and increments it for the next run."""
        with self.lock:
            row = self._one("SELECT value FROM meta WHERE key = 'cycle'")
            n = int(row[0]) if row else 0
            self.db.execute(
                "INSERT INTO meta (key, value) VALUES ('cycle', ?) "
                "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (n + 1,),
            )
            return n

    def get_kv(self, key: str) -> Optional[str]:
        with self.lock:
            r = self._one("SELECT value FROM kv WHERE key = ?", key)
        return r[0] if r else None

    def set_kv(self, key: str, value: str) -> None:
        with self.lock:
            self.db.execute(
                "INSERT INTO kv (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                (key, value),
            )

    def retry_stuck(self, max_attempts: int) -> int:
        """Give every stuck ZIP (attempts >= max_attempts) a fresh set of attempts."""
        with self.lock:
            cur = self.db.execute("UPDATE spool SET attempts = 0 WHERE attempts >= ?", (max_attempts,))
            return cur.rowcount

    def get_archived(self, host: str, path: str, thread_id: str) -> Optional[dict]:
        with self.lock:
            r = self._one(
                "SELECT replies, archived_at FROM archived WHERE host = ? AND path = ? AND thread_id = ?",
                host, path, thread_id,
            )
        return {"replies": r[0], "archived_at": r[1]} if r else None

    def get_spooled(self, host: str, path: str, thread_id: str) -> Optional[dict]:
        """Highest reply count among spooled (built, not yet uploaded) zips for this thread."""
        with self.lock:
            r = self._one(
                "SELECT MAX(replies) FROM spool WHERE host = ? AND path = ? AND thread_id = ?",
                host, path, thread_id,
            )
        return {"replies": r[0]} if r and r[0] is not None else None

    def record_archived(self, host, path, thread_id, replies, ia_item=None, ia_file=None, archived_at=None) -> None:
        with self.lock:
            self.db.execute(
                """INSERT INTO archived (host, path, thread_id, replies, archived_at, ia_item, ia_file)
                   VALUES (?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT (host, path, thread_id) DO UPDATE SET
                     replies = excluded.replies, archived_at = excluded.archived_at,
                     ia_item = excluded.ia_item, ia_file = excluded.ia_file""",
                (host, path, thread_id, replies, archived_at if archived_at is not None else now_ms(), ia_item, ia_file),
            )

    def import_archived(self, rows: list) -> int:
        """Bulk import. rows: dicts with host, path, thread_id, replies, archived_at (+ optional
        ia_item, ia_file). Never lowers an existing reply count; the IA location is only replaced
        by a row that is at least as new."""
        with self.lock:
            self.db.execute("BEGIN")
            try:
                for r in rows:
                    self.db.execute(
                        """INSERT INTO archived (host, path, thread_id, replies, archived_at, ia_item, ia_file)
                           VALUES (?, ?, ?, ?, ?, ?, ?)
                           ON CONFLICT (host, path, thread_id) DO UPDATE SET
                             ia_item = CASE WHEN excluded.ia_item IS NOT NULL AND excluded.replies >= archived.replies
                                            THEN excluded.ia_item ELSE archived.ia_item END,
                             ia_file = CASE WHEN excluded.ia_item IS NOT NULL AND excluded.replies >= archived.replies
                                            THEN excluded.ia_file ELSE archived.ia_file END,
                             replies = MAX(archived.replies, excluded.replies)""",
                        (r["host"], r["path"], r["thread_id"], r["replies"], r["archived_at"],
                         r.get("ia_item"), r.get("ia_file")),
                    )
                self.db.execute("COMMIT")
            except Exception:
                self.db.execute("ROLLBACK")
                raise
        return len(rows)

    def add_spool(self, row: SpoolRow) -> None:
        with self.lock:
            self.db.execute(
                """INSERT OR REPLACE INTO spool (file, host, path, thread_id, replies, size, created_at, attempts, last_error)
                   VALUES (?, ?, ?, ?, ?, ?, ?, 0, NULL)""",
                (row.file, row.host, row.path, row.thread_id, row.replies, row.size, row.created_at),
            )

    def list_spool(self, limit: int, max_attempts: int) -> list:
        """Oldest first. Rows that failed with a NON-retryable error `max_attempts`
        times are skipped (reported as "stuck" in stats) so one bad file can't
        block the queue. Retryable failures never count toward that limit."""
        with self.lock:
            rows = self.db.execute(
                """SELECT file, host, path, thread_id, replies, size, created_at, attempts, last_error
                   FROM spool WHERE attempts < ? ORDER BY created_at ASC LIMIT ?""",
                (max_attempts, limit),
            ).fetchall()
        return [SpoolRow(*r) for r in rows]

    def note_spool_error(self, file: str, error: str, count_attempt: bool) -> None:
        with self.lock:
            self.db.execute(
                "UPDATE spool SET last_error = ?, attempts = attempts + ? WHERE file = ?",
                (error[:500], 1 if count_attempt else 0, file),
            )

    def remove_spool(self, file: str) -> None:
        with self.lock:
            self.db.execute("DELETE FROM spool WHERE file = ?", (file,))

    def spool_bytes(self) -> int:
        with self.lock:
            return int(self._one("SELECT COALESCE(SUM(size), 0) FROM spool")[0])

    def stats(self, max_attempts: int) -> dict:
        day = now_ms() - 24 * 3600 * 1000
        with self.lock:
            oldest = self._one("SELECT MIN(created_at) FROM spool")[0]
            return {
                "archivedTotal": int(self._one("SELECT COUNT(*) FROM archived")[0]),
                "archivedLast24h": int(self._one("SELECT COUNT(*) FROM archived WHERE archived_at >= ?", day)[0]),
                "spoolFiles": int(self._one("SELECT COUNT(*) FROM spool")[0]),
                "spoolBytes": self.spool_bytes(),
                "spoolStuck": int(self._one("SELECT COUNT(*) FROM spool WHERE attempts >= ?", max_attempts)[0]),
                "oldestSpoolAgeMinutes": round((now_ms() - oldest) / 60000) if oldest else None,
            }
