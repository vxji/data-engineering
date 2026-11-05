"""Incremental, idempotent loaders for the two feeds that keep growing.

Pattern (Labs 2.1-2.3): read a cursor, fetch what changed, then write the rows and the new cursor
in one transaction. Cursors are inclusive, so a little overlap is expected and harmless: the key
makes it a no-op. A crash before COMMIT leaves both the rows and the cursor untouched.
"""

from __future__ import annotations

import random
import time
from datetime import datetime, timezone

import pandas as pd
import requests

import de_utils as U

PAYMENT_COLS = ["payment_id", "order_id", "amount", "currency", "method", "status", "card_last4",
                "wallet_provider", "created_at", "updated_at"]
PAYMENTS_START = "2026-08-01T00:00:00.000Z"
EVENTS_START = "2026-01-01T00:00:00.000Z"


def now_utc() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def ensure_ops(con) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    con.execute("CREATE SCHEMA IF NOT EXISTS ops")
    con.execute("CREATE TABLE IF NOT EXISTS ops.ingest_state (source VARCHAR PRIMARY KEY, cursor VARCHAR, updated_at TIMESTAMP)")
    con.execute("""CREATE TABLE IF NOT EXISTS ops.ingest_log (
        logged_at TIMESTAMP, source VARCHAR, as_of VARCHAR, cursor_before VARCHAR, cursor_after VARCHAR,
        rows_fetched BIGINT, rows_new BIGINT, rows_updated BIGINT, rows_unchanged BIGINT, requests BIGINT, throttled BIGINT)""")


def read_cursor(con, source: str, default: str) -> str:
    rows = con.execute("SELECT cursor FROM ops.ingest_state WHERE source = ?", [source]).fetchall()
    return rows[0][0] if rows else default


def write_cursor(con, source: str, cursor: str) -> None:
    con.execute("DELETE FROM ops.ingest_state WHERE source = ?", [source])
    con.execute("INSERT INTO ops.ingest_state VALUES (?, ?, ?)", [source, cursor, now_utc()])


def get_with_retry(url: str, params: dict, tries: int = 6, timeout: int = 10) -> dict:
    """GET with backoff and jitter. 429 and 5xx are worth retrying; other errors are bugs, so raise."""
    for attempt in range(tries):
        r = requests.get(url, params=params, timeout=timeout)
        if r.status_code == 429 or r.status_code >= 500:
            wait = float(r.headers.get("Retry-After", 2 ** attempt))
            time.sleep(wait + random.uniform(0, 0.25))
            continue
        r.raise_for_status()
        return r.json()
    raise RuntimeError(f"gave up after {tries} tries: {url}")


def fetch_payments(since: str, clock: str | None) -> tuple[list[dict], int, int]:
    """Every page of payments changed since the cursor, from the provider's HTTP API."""
    api = U.PaymentsAPI(page_size=500, rate_limit_every=25, clock=clock)
    base = api.start()
    try:
        rows, page = [], 1
        while page:
            body = get_with_retry(f"{base}/payments", {"updated_since": since, "page": page})
            rows += body["data"]
            page = body["next_page"]
        return rows, api.requests, api.throttled
    finally:
        api.stop()


def load_payments(db, clock: str | None = None, fault: str | None = None) -> dict:
    """Upsert changed payments on payment_id. The rows and the cursor commit together."""
    con = U.connect(db)
    try:
        ensure_ops(con)
        con.execute("""CREATE TABLE IF NOT EXISTS raw.payments (
            payment_id VARCHAR, order_id BIGINT, amount DOUBLE, currency VARCHAR, method VARCHAR, status VARCHAR,
            card_last4 VARCHAR, wallet_provider VARCHAR, created_at VARCHAR, updated_at VARCHAR, _loaded_at TIMESTAMP)""")
        since = read_cursor(con, "payments", PAYMENTS_START)
        rows, requests_made, throttled = fetch_payments(since, clock)
        if not rows:
            return {"fetched": 0, "new": 0, "updated": 0, "unchanged": 0, "cursor": since, "unknown_fields": "none"}
        if fault == "bad-status":                 # demo only: the provider starts sending an unagreed status
            rows[0]["status"] = "refund_pending"
        unknown = sorted(set().union(*rows) - set(PAYMENT_COLS))
        batch = pd.DataFrame(rows).reindex(columns=PAYMENT_COLS)
        batch["card_last4"] = batch["card_last4"].astype("string")
        batch["wallet_provider"] = batch["wallet_provider"].astype("string")
        new_cursor = max(r["updated_at"] for r in rows)
        con.register("batch", batch)
        new, updated, unchanged = con.execute("""
            SELECT count(*) FILTER (WHERE p.payment_id IS NULL),
                   count(*) FILTER (WHERE p.payment_id IS NOT NULL AND p.updated_at <> b.updated_at),
                   count(*) FILTER (WHERE p.payment_id IS NOT NULL AND p.updated_at = b.updated_at)
            FROM batch b LEFT JOIN raw.payments p USING (payment_id)""").fetchall()[0]
        con.execute("BEGIN")
        try:
            con.execute("DELETE FROM raw.payments WHERE payment_id IN (SELECT payment_id FROM batch)")
            con.execute(f"INSERT INTO raw.payments SELECT {', '.join(PAYMENT_COLS[:7])}, wallet_provider, created_at, updated_at, ? FROM batch", [now_utc()])
            write_cursor(con, "payments", new_cursor)
            con.execute("INSERT INTO ops.ingest_log VALUES (?, 'payments', ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        [now_utc(), clock, since, new_cursor, len(rows), new, updated, unchanged, requests_made, throttled])
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
        return {"fetched": len(rows), "new": new, "updated": updated, "unchanged": unchanged, "requests": requests_made,
                "throttled": throttled, "cursor": new_cursor, "unknown_fields": ", ".join(unknown) or "none"}
    finally:
        con.close()


def load_events(db, clock: str | None = None) -> dict:
    """Append courier events by arrival time. Bronze keeps redeliveries; staging removes them.

    The cursor is ingest_time, not event_time: a courier offline for an hour still arrives after the
    cursor. (event_id, ingest_time) identifies one delivery, so overlap inserts nothing.
    """
    source = (U.RAW / "events" / "delivery_events.jsonl").as_posix()
    reader = f"""read_json('{source}', format='newline_delimited', columns={{
        event_id: 'VARCHAR', order_id: 'BIGINT', store_id: 'VARCHAR', city: 'VARCHAR', courier_id: 'VARCHAR',
        event_type: 'VARCHAR', event_time: 'VARCHAR', lat: 'DOUBLE', lon: 'DOUBLE', ingest_time: 'VARCHAR'}})"""
    con = U.connect(db)
    try:
        ensure_ops(con)
        con.execute("""CREATE TABLE IF NOT EXISTS raw.delivery_events (
            event_id VARCHAR, order_id BIGINT, store_id VARCHAR, city VARCHAR, courier_id VARCHAR, event_type VARCHAR,
            event_time VARCHAR, lat DOUBLE, lon DOUBLE, ingest_time VARCHAR, _loaded_at TIMESTAMP)""")
        since = read_cursor(con, "delivery_events", EVENTS_START)
        upper = "AND ingest_time::TIMESTAMPTZ <= ?::TIMESTAMPTZ" if clock else ""
        params = [since] + ([clock] if clock else [])
        con.execute(f"""CREATE OR REPLACE TEMP TABLE batch AS
            SELECT * FROM {reader} WHERE ingest_time::TIMESTAMPTZ >= ?::TIMESTAMPTZ {upper}""", params)
        fetched = con.execute("SELECT count(*) FROM batch").fetchall()[0][0]
        new = con.execute("""SELECT count(*) FROM batch b WHERE NOT EXISTS (
            SELECT 1 FROM raw.delivery_events e WHERE e.event_id = b.event_id AND e.ingest_time = b.ingest_time)""").fetchall()[0][0]
        cursor = con.execute("SELECT max(ingest_time::TIMESTAMPTZ)::VARCHAR FROM batch").fetchall()[0][0] or since
        con.execute("BEGIN")
        try:
            con.execute("""INSERT INTO raw.delivery_events SELECT b.*, ? FROM batch b WHERE NOT EXISTS (
                SELECT 1 FROM raw.delivery_events e WHERE e.event_id = b.event_id AND e.ingest_time = b.ingest_time)""", [now_utc()])
            write_cursor(con, "delivery_events", cursor)
            con.execute("INSERT INTO ops.ingest_log VALUES (?, 'delivery_events', ?, ?, ?, ?, ?, 0, ?, 0, 0)",
                        [now_utc(), clock, since, cursor, fetched, new, fetched - new])
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
        return {"fetched": fetched, "new": new, "unchanged": fetched - new, "cursor": cursor}
    finally:
        con.close()
