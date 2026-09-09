"""SQLite `WaitStore`.

Useful in three places: a single-machine deployment, a local run of the example agent
that survives restarts, and -- most importantly -- as a second implementation for the
conformance suite. A rule that passes only against the in-memory store has not been
tested; it has been asserted.

The record is a JSON document with the queryable fields lifted into columns. The
compare-and-set that the whole design needs is `UPDATE ... WHERE status = :expect`
inside `BEGIN IMMEDIATE`, which SQLite gives us honestly.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from collections.abc import Generator, Mapping
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from typing import Any

from ..model import Lease, Status, Wait

_SCHEMA = """
CREATE TABLE IF NOT EXISTS waits (
    wait_id         TEXT PRIMARY KEY,
    thread_id       TEXT NOT NULL,
    status          TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    notified_at     REAL,
    expires_at      REAL,
    created_at      REAL NOT NULL,
    version         INTEGER NOT NULL,
    doc             TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS waits_by_thread ON waits (thread_id);
CREATE INDEX IF NOT EXISTS waits_by_status ON waits (status, expires_at);

CREATE TABLE IF NOT EXISTS applied (
    thread_id  TEXT NOT NULL,
    message_id TEXT NOT NULL,
    expires_at REAL,
    PRIMARY KEY (thread_id, message_id)
);

CREATE TABLE IF NOT EXISTS leases (
    thread_id  TEXT PRIMARY KEY,
    owner      TEXT NOT NULL,
    expires_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS parked (
    key        TEXT PRIMARY KEY,
    payload    TEXT NOT NULL,
    expires_at REAL
);
"""


class SqliteWaitStore:
    def __init__(self, path: str | Path = ":memory:") -> None:
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(str(path), check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.executescript(_SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    @contextmanager
    def _tx(self) -> Generator[sqlite3.Connection]:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                yield self._conn
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise
            else:
                self._conn.execute("COMMIT")

    @staticmethod
    def _row_to_wait(row: sqlite3.Row) -> Wait:
        return Wait.from_dict(json.loads(row["doc"]))

    def _write(self, conn: sqlite3.Connection, wait: Wait) -> None:
        conn.execute(
            "UPDATE waits SET thread_id=?, status=?, notified_at=?, expires_at=?, version=?, doc=? "
            "WHERE wait_id=?",
            (
                wait.thread_id,
                wait.status,
                wait.notified_at,
                wait.expires_at,
                wait.version,
                json.dumps(wait.to_dict(), default=str),
                wait.wait_id,
            ),
        )

    # -- waits ---------------------------------------------------------------
    def create(self, wait: Wait) -> tuple[Wait, bool]:
        with self._tx() as conn:
            try:
                conn.execute(
                    "INSERT INTO waits (wait_id, thread_id, status, idempotency_key, notified_at, "
                    "expires_at, created_at, version, doc) VALUES (?,?,?,?,?,?,?,?,?)",
                    (
                        wait.wait_id,
                        wait.thread_id,
                        wait.status,
                        wait.idempotency_key,
                        wait.notified_at,
                        wait.expires_at,
                        wait.created_at,
                        wait.version,
                        json.dumps(wait.to_dict(), default=str),
                    ),
                )
                return wait, True
            except sqlite3.IntegrityError:
                row = conn.execute(
                    "SELECT doc FROM waits WHERE idempotency_key = ?", (wait.idempotency_key,)
                ).fetchone()
                if row is None:  # pragma: no cover - a duplicate wait_id, not idempotency key
                    raise
                return self._row_to_wait(row), False

    def get(self, wait_id: str) -> Wait | None:
        with self._lock:
            row = self._conn.execute("SELECT doc FROM waits WHERE wait_id = ?", (wait_id,)).fetchone()
            return self._row_to_wait(row) if row is not None else None

    def find(
        self,
        *,
        status: Status | None = None,
        thread_id: str | None = None,
        notified: bool | None = None,
        due_before: float | None = None,
        limit: int | None = None,
    ) -> list[Wait]:
        sql = "SELECT doc FROM waits WHERE 1=1"
        params: list[Any] = []
        if status is not None:
            sql += " AND status = ?"
            params.append(status)
        if thread_id is not None:
            sql += " AND thread_id = ?"
            params.append(thread_id)
        if notified is True:
            sql += " AND notified_at IS NOT NULL"
        elif notified is False:
            sql += " AND notified_at IS NULL"
        if due_before is not None:
            sql += " AND expires_at IS NOT NULL AND expires_at <= ?"
            params.append(due_before)
        sql += " ORDER BY created_at ASC"
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()
        return [self._row_to_wait(r) for r in rows]

    def transition(self, wait_id: str, *, expect: Status, to: Status, **fields: Any) -> bool:
        with self._tx() as conn:
            row = conn.execute(
                "SELECT doc FROM waits WHERE wait_id = ? AND status = ?", (wait_id, expect)
            ).fetchone()
            if row is None:
                return False
            current = self._row_to_wait(row)
            updated = replace(current, status=to, version=current.version + 1, **fields)
            self._write(conn, updated)
            return True

    def set_fields(self, wait_id: str, **fields: Any) -> None:
        with self._tx() as conn:
            row = conn.execute("SELECT doc FROM waits WHERE wait_id = ?", (wait_id,)).fetchone()
            if row is None:
                return
            self._write(conn, replace(self._row_to_wait(row), **fields))

    # -- applied messages ----------------------------------------------------
    def record_applied(self, thread_id: str, message_id: str, *, ttl: float | None = None) -> bool:
        with self._tx() as conn:
            cur = conn.execute(
                "INSERT OR IGNORE INTO applied (thread_id, message_id, expires_at) VALUES (?,?,?)",
                (thread_id, message_id, ttl),
            )
            return cur.rowcount == 1

    # -- leases --------------------------------------------------------------
    def acquire_lease(self, thread_id: str, owner: str, *, expires_at: float, now: float) -> bool:
        with self._tx() as conn:
            row = conn.execute(
                "SELECT owner, expires_at FROM leases WHERE thread_id = ?", (thread_id,)
            ).fetchone()
            if row is not None and row["owner"] != owner and row["expires_at"] > now:
                return False
            conn.execute(
                "INSERT INTO leases (thread_id, owner, expires_at) VALUES (?,?,?) "
                "ON CONFLICT(thread_id) DO UPDATE SET owner=excluded.owner, expires_at=excluded.expires_at",
                (thread_id, owner, expires_at),
            )
            return True

    def refresh_lease(self, thread_id: str, owner: str, *, expires_at: float) -> bool:
        with self._tx() as conn:
            cur = conn.execute(
                "UPDATE leases SET expires_at = ? WHERE thread_id = ? AND owner = ?",
                (expires_at, thread_id, owner),
            )
            return cur.rowcount == 1

    def release_lease(self, thread_id: str, owner: str) -> None:
        with self._tx() as conn:
            conn.execute("DELETE FROM leases WHERE thread_id = ? AND owner = ?", (thread_id, owner))

    def get_lease(self, thread_id: str) -> Lease | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT owner, expires_at FROM leases WHERE thread_id = ?", (thread_id,)
            ).fetchone()
        return Lease(thread_id, row["owner"], row["expires_at"]) if row is not None else None

    # -- parked answers ------------------------------------------------------
    def park_answer(self, key: str, payload: Mapping[str, Any], *, ttl: float | None = None) -> None:
        with self._tx() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO parked (key, payload, expires_at) VALUES (?,?,?)",
                (key, json.dumps(dict(payload), default=str), ttl),
            )

    def take_parked_answer(self, key: str) -> dict[str, Any] | None:
        with self._tx() as conn:
            row = conn.execute("SELECT payload FROM parked WHERE key = ?", (key,)).fetchone()
            if row is None:
                return None
            conn.execute("DELETE FROM parked WHERE key = ?", (key,))
            loaded: dict[str, Any] = json.loads(row["payload"])
            return loaded
