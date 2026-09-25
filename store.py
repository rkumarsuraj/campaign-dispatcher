"""
Persistence layer. All database access lives here.

TWO THINGS TO UNDERSTAND ABOUT THIS FILE:

1. THE IDEMPOTENCY GUARANTEE IS A DATABASE CONSTRAINT, NOT PYTHON CODE.
   `attempts` has PRIMARY KEY (campaign_id, contact_id, attempt_no). `claim_attempt`
   INSERTs a row with status='in_flight' *before* the call is placed. If the insert
   raises IntegrityError, an attempt with that exact key already exists and this
   caller must not dial. The check ("does it exist?") and the claim ("it's mine")
   are a single atomic write, so there is no window between them for a second
   concurrent caller to slip into. Same mechanism as an idempotency key on a
   payments API preventing a double charge.

2. sqlite3 IS BLOCKING, SO EVERY CALL GOES THROUGH asyncio.to_thread.
   Calling sqlite3 directly from a coroutine blocks the event loop, which stalls
   every other in-flight call. At 300 contacts each write is well under a
   millisecond so it would be practically invisible -- but it is wrong in
   principle and stops being invisible the moment the database is remote.
   A threading.Lock serialises access to the single connection.

   That lock is also an honest preview of the scaling problem: SQLite is a
   single-writer database. Serialising writes is fine in one process and fatal
   across many, which is exactly why the scaling answer moves to Postgres --
   keeping this same unique-constraint mechanism, which ports over unchanged.

ON TIMESTAMPS: every timestamp is stored twice -- ISO-8601 UTC for humans reading
the table, and epoch milliseconds for arithmetic. Doing time maths on ISO strings
in SQLite means julianday() gymnastics; an integer column makes the analytics
queries plain.
"""

from __future__ import annotations

import asyncio
import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime, timezone

from policy import Resolution
from provider import Disposition

SCHEMA = """
PRAGMA journal_mode = WAL;
PRAGMA busy_timeout = 5000;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS contacts (
    contact_id   TEXT PRIMARY KEY,
    phone_number TEXT NOT NULL,
    name         TEXT,
    account_ref  TEXT
);

CREATE TABLE IF NOT EXISTS campaigns (
    campaign_id  TEXT PRIMARY KEY,
    created_at   TEXT NOT NULL,
    config_json  TEXT NOT NULL
);

-- One row per call ATTEMPT. The primary key is the idempotency key.
CREATE TABLE IF NOT EXISTS attempts (
    campaign_id      TEXT    NOT NULL,
    contact_id       TEXT    NOT NULL,
    attempt_no       INTEGER NOT NULL,
    status           TEXT    NOT NULL,   -- 'in_flight' | 'completed'
    disposition      TEXT,               -- NULL while in_flight
    dispatched_at    TEXT    NOT NULL,   -- ISO-8601 UTC
    dispatched_at_ms INTEGER NOT NULL,   -- epoch ms, for arithmetic
    completed_at     TEXT,
    completed_at_ms  INTEGER,
    latency_ms       INTEGER,            -- provider call duration ONLY
    PRIMARY KEY (campaign_id, contact_id, attempt_no)
);

CREATE INDEX IF NOT EXISTS idx_attempts_contact
    ON attempts (campaign_id, contact_id);
CREATE INDEX IF NOT EXISTS idx_attempts_disposition
    ON attempts (campaign_id, disposition);

-- One row per contact, written once when the contact stops being dialled.
-- Not strictly derivable from `attempts` without re-running the policy, so it is
-- recorded explicitly: it is what makes "exhausted retries" queryable.
CREATE TABLE IF NOT EXISTS contact_outcomes (
    campaign_id   TEXT    NOT NULL,
    contact_id    TEXT    NOT NULL,
    resolution    TEXT    NOT NULL,   -- answered | terminal_disposition | exhausted
    final_attempt INTEGER NOT NULL,
    resolved_at   TEXT    NOT NULL,
    PRIMARY KEY (campaign_id, contact_id)
);
"""


@dataclass(frozen=True)
class Contact:
    contact_id: str
    phone_number: str
    name: str
    account_ref: str


def _now() -> tuple[str, int]:
    """(ISO-8601 UTC, epoch milliseconds)."""
    dt = datetime.now(timezone.utc)
    return dt.isoformat(), int(dt.timestamp() * 1000)


class Store:
    def __init__(self, path: str = "campaign.db") -> None:
        self.path = path
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.Lock()
        with self._lock:
            self._conn.executescript(SCHEMA)
            self._conn.commit()

    # ---------- sync internals (always called inside a worker thread) ----------

    def _claim_attempt_sync(
        self, campaign_id: str, contact_id: str, attempt_no: int
    ) -> bool:
        iso, ms = _now()
        with self._lock:
            try:
                self._conn.execute(
                    """
                    INSERT INTO attempts (
                        campaign_id, contact_id, attempt_no, status,
                        dispatched_at, dispatched_at_ms
                    ) VALUES (?, ?, ?, 'in_flight', ?, ?)
                    """,
                    (campaign_id, contact_id, attempt_no, iso, ms),
                )
                self._conn.commit()
                return True
            except sqlite3.IntegrityError:
                # An attempt with this exact key already exists. Someone else owns
                # it. Do NOT dial.
                self._conn.rollback()
                return False

    def _record_attempt_sync(
        self,
        campaign_id: str,
        contact_id: str,
        attempt_no: int,
        disposition: Disposition,
        latency_ms: int,
    ) -> None:
        iso, ms = _now()
        with self._lock:
            self._conn.execute(
                """
                UPDATE attempts
                   SET status = 'completed',
                       disposition = ?,
                       completed_at = ?,
                       completed_at_ms = ?,
                       latency_ms = ?
                 WHERE campaign_id = ? AND contact_id = ? AND attempt_no = ?
                """,
                (
                    disposition.value,
                    iso,
                    ms,
                    latency_ms,
                    campaign_id,
                    contact_id,
                    attempt_no,
                ),
            )
            self._conn.commit()

    def _record_outcome_sync(
        self,
        campaign_id: str,
        contact_id: str,
        resolution: Resolution,
        final_attempt: int,
    ) -> None:
        iso, _ = _now()
        with self._lock:
            self._conn.execute(
                """
                INSERT OR IGNORE INTO contact_outcomes (
                    campaign_id, contact_id, resolution, final_attempt, resolved_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (campaign_id, contact_id, resolution.value, final_attempt, iso),
            )
            self._conn.commit()

    # ---------------------------- async API ----------------------------

    async def claim_attempt(
        self, campaign_id: str, contact_id: str, attempt_no: int
    ) -> bool:
        """
        Atomically claim the right to dial (campaign_id, contact_id, attempt_no).

        Returns True if this caller won the claim and must dial, False if the
        attempt was already claimed and this caller must NOT dial.
        """
        return await asyncio.to_thread(
            self._claim_attempt_sync, campaign_id, contact_id, attempt_no
        )

    async def record_attempt(
        self,
        campaign_id: str,
        contact_id: str,
        attempt_no: int,
        disposition: Disposition,
        latency_ms: int,
    ) -> None:
        await asyncio.to_thread(
            self._record_attempt_sync,
            campaign_id,
            contact_id,
            attempt_no,
            disposition,
            latency_ms,
        )

    async def record_outcome(
        self,
        campaign_id: str,
        contact_id: str,
        resolution: Resolution,
        final_attempt: int,
    ) -> None:
        await asyncio.to_thread(
            self._record_outcome_sync,
            campaign_id,
            contact_id,
            resolution,
            final_attempt,
        )

    # -------------------------- setup / teardown --------------------------

    def create_campaign(self, campaign_id: str, config_json: str) -> None:
        iso, _ = _now()
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO campaigns (campaign_id, created_at, config_json)"
                " VALUES (?, ?, ?)",
                (campaign_id, iso, config_json),
            )
            self._conn.commit()

    def insert_contacts(self, contacts: list[Contact]) -> None:
        with self._lock:
            self._conn.executemany(
                "INSERT OR REPLACE INTO contacts"
                " (contact_id, phone_number, name, account_ref) VALUES (?, ?, ?, ?)",
                [(c.contact_id, c.phone_number, c.name, c.account_ref) for c in contacts],
            )
            self._conn.commit()

    def query(self, sql: str, params: tuple = ()) -> list[sqlite3.Row]:
        """Read-only helper used by the analytics layer."""
        with self._lock:
            return self._conn.execute(sql, params).fetchall()

    def close(self) -> None:
        with self._lock:
            self._conn.close()
