# Architecture decisions

Each decision records what we chose, what we rejected and what it costs.

## Option 1, Qafila Lakehouse
We first looked at Saudi open data. The portal's dataset links were unreliable from our network, so we chose the option with reliable, permitted data and put the time into engineering.

## DuckDB as the warehouse
- **Why:** no server, strong analytics, works on a laptop and in Colab, so anyone can reproduce the project.
- **Rejected:** a cloud warehouse, which adds accounts and cost and does nothing for this data size.
- **Cost:** one writer at a time and one machine. Assets open short-lived connections for this reason.

## A hand-written loader instead of dlt
- **Why:** we wanted the rows and the cursor in one visible transaction, and to keep the raw schema explicit. The loader is about 100 lines.
- **Rejected:** dlt, which would remove code but infer types and add its own columns, and the contract checks the raw text format.
- **Cost:** schema evolution is ours to handle. We keep a fixed column list and record unknown fields in the run metadata, so nothing is dropped silently without a trace.

## Cursor on `ingest_time` for events
- **Why:** arrival order is what the loader sees, so a late event is never missed. Business time stays in `event_time` for analysis.
- **Rejected:** a cursor on `event_time`, which would skip events from couriers who were offline.

## Inclusive cursors
- **Why:** two rows can share a timestamp, and a strict `>` could skip the second one. We overlap a little and make the load idempotent.
- **Cost:** each run fetches at least one row it already holds.

## Full snapshots for app tables
- **Why:** these tables are small, and a snapshot notices hard deletes, which a cursor can't see. PDPL erasure depends on this.
- **Cost:** it won't scale. Change data capture is the next step (Lab 2.3).

## Three outcomes for bad data
- **Why:** rejecting a feed for 16 rows leaves finance with nothing, and loading them corrupts `paid_amount`.
- **Rule:** block, quarantine or warn, by comparing the cost of a wrong number with the cost of a late one.
- **Threshold:** quarantine holds up to 1% of the feed. Above that, the problem is the feed, not the rows.

## A vault at the ingestion boundary
- **Why:** if identifiers never enter the warehouse, no later model, view or backup can leak them.
- **Rejected:** masking views over a warehouse that holds identifiers, because one new tool could bypass them.
- **Cost:** support needs a separate path, which is the `support.customers` view in the vault.

## Iceberg for gold only
- **Why:** gold tables are the interface to other engines, and an open format keeps that door open. They also add snapshots.
- **Rejected:** Iceberg everywhere, which is more machinery than a single-machine project needs.

## Dagster
- **Why:** assets match how we think about tables, dbt tests become checks without extra code, and blocking checks stop downstream assets.
- **Rejected:** Airflow, which is task-based and heavier to run for a two-person project.
