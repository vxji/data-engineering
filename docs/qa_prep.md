# Technical Q&A preparation

**Why is your load idempotent?** Payments upsert on `payment_id`, and events skip a key they already hold. The cursor is inclusive, so the newest row returns each time, and the key makes that harmless. `prove_rerun.py` compares a hash of every row across two runs.

**What if the job dies mid-load?** The rows and the cursor are in one transaction. If it dies before COMMIT, both stay as they were, and the next run fetches the same rows.

**Why not cursor on `event_time`?** A courier offline for an hour sends old event times, and a cursor on `event_time` would skip them. `ingest_time` is when they reached us.

**Why keep redelivered events in raw?** Bronze records what arrived. `stg_delivery_events` keeps one row per `event_id`.

**How do you see deletes?** App tables are full snapshots, so a deleted row disappears. A cursor couldn't see that. At larger scale we'd use change data capture.

**What blocks, and why?** The payments contract, the vault boundary, and the PII scan block. So do key and relationship tests in the marts. We block when a wrong number or a leak costs more than a late one.

**What happens to a row after a block?** It stays in `raw.payments`, and the marts keep their last build. That's a known limit: we'd validate the batch before commit, or wait for the provider to resend it.

**Why quarantine instead of dropping rows?** Dropped rows are silent. Quarantine keeps the row, the reason and an owner, and a test proves raw equals clean plus quarantined.

**Why does the check allow 1% in quarantine?** A few rows are a row problem. Above 1%, the feed is broken, so we stop.

**Why is a plain hash not enough?** Anyone with a list of national IDs can hash them and match. An HMAC needs a secret key.

**Is tokenized data anonymous?** No. It's pseudonymized, so it's still personal data under PDPL, because the key holder can link it.

**Where's the key?** In an environment variable, or a random file in the run folder that git ignores. In production it belongs in a secret manager.

**What if the key changes?** Every token changes, so all tokens must be rebuilt together.

**Why is `city` Restricted?** Next to other columns, it can help identify a person. That's why analysts don't get `district`.

**What is the grain of `fct_order_items`?** One row per order line with a positive quantity.

**How do you avoid fan-out with delivery fees?** Fees are at order grain, so `fct_orders` rolls lines up to one row per order first and adds the fee once.

**Why does the fact keep erased customers' orders?** Finance still needs the sale. The order points to the unknown member, `-1`, with no personal link.

**Why was `items_amount` rounded?** Summing doubles depends on row order, so two identical runs differed in the last digits. The hash proof caught it.

**What breaks first at 100× the volume?** The full snapshots and the single DuckDB writer. We'd use change data capture, and Iceberg as the system of record.

**Why not dlt?** We wanted the rows and the cursor visible in one transaction, and a fixed raw schema for the contract. It costs us handling schema changes ourselves.

**Are the payments truly from an API?** Yes, over HTTP with pagination and retries, against the course's local stand-in for the provider.
