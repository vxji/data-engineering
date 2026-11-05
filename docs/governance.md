# Governance note

This is an engineering exercise, not legal advice. Levels and rules belong to an organization's policies and its data protection officer. The levels below are an illustrative Qafila policy using the NDMO levels.

## Principles
1. **Keep identifiers out.** Analysis doesn't need them, so they never enter the warehouse.
2. **Protect at the boundary.** Controls sit at ingestion, not in a later cleanup.
3. **Check by value.** A column name proves nothing, so the scanner reads the values.
4. **Least privilege.** Each role reads only what its job needs.

## Data flow
1. The `raw/customers` asset reads the app's table.
2. Identifiers go to `vault.duckdb`, a separate file.
3. The warehouse receives `person_token`, an HMAC-SHA256 of the national ID, plus safe columns.
4. Dagster blocks the run if the scanner finds identifiers in `raw.customers`.
5. Analysts read the `analyst` schema. The scanner must find nothing there and in `marts`, or gold is not published.

## Column classification
The same list is published as `ops.data_catalog`, from `labs/capstone/governance/catalog.yaml`.

| Location | Column | Personal data | Level | Protection | Raw access |
|---|---|---|---|---|---|
| vault | `national_id` | National ID | Secret | Vault only; token in the warehouse | Data protection officer |
| vault | `iban` | Bank account | Secret | Vault only | Data protection officer |
| vault | `full_name` | Name | Restricted | Vault only; support view | Support |
| vault | `email` | Email | Restricted | Vault only; domain in the warehouse | Support |
| vault | `mobile` | Mobile | Restricted | Vault only; masked in the support view | Nobody by default |
| raw | `person_token` | Pseudonym | Restricted | Keyed hash; key outside git | Data engineers |
| raw | `email_domain` | Indirect | Restricted | Left out of analyst views | Data engineers |
| raw | `city` | Indirect | Restricted | Personal by association | Analysts |
| raw | `district` | Indirect | Restricted | Left out of analyst views | Data engineers |
| raw | `card_last4` | Partial card number | Restricted | Stops at staging | Data engineers |
| raw | `lat`, `lon` | Customer location | Restricted | Stops at staging | Data engineers |
| raw | `courier_id` | Employee identifier | Restricted | Not in analyst views | Data engineers |
| raw | `review_text` | Free text | Restricted | Scanned every run; not in marts | Data engineers |

## Pseudonymization
- **Method:** HMAC-SHA256 under a secret key, first 20 hex characters.
- **Why not a plain hash:** anyone with a list of IDs can hash them and match. A keyed hash needs the key.
- **Key:** read from `QAFILA_PSEUDO_KEY`. Without it, a random key is created in the run folder, which git ignores. A real deployment uses a secret manager.
- **Key change:** every token changes, so joins across old and new tokens break. Tokens must be rebuilt together.
- **Status:** pseudonymized data is still personal data under PDPL, because whoever holds the key can match it.

## Role views
| Role | Reads | Does not read |
|---|---|---|
| Analyst | `analyst.customers`, `orders`, `daily_city_sales`, `ops_daily` | Tokens, district, delivery locations, raw, staging, quarantine |
| Support | `support.customers` in the vault | National ID, IBAN, full mobile |
| Data engineer | Warehouse | Vault |

DuckDB has no column-level security, so views model the role and the separate vault file is the real boundary. A warehouse would enforce this with grants and masking policies.

## Erasure
1. The customer is deleted in the app.
2. The nightly snapshot removes them from `raw.customers` and the vault.
3. Their orders stay for finance and point to the unknown member, `-1`.
4. The token no longer exists, so nothing links back.

**Limitation:** backups and old Iceberg snapshots need a retention rule. Gold tables hold no customer-level data, which limits the exposure.
