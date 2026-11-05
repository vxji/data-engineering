"""Privacy by design: direct identifiers never enter the warehouse.

customers splits at the ingestion boundary. Raw identifiers go to a separate vault file; the warehouse
gets a keyed token (HMAC-SHA256) that still links the accounts of one person. Roles read views.
Pseudonymized data is still personal data under PDPL: the token lowers the risk, it does not remove it.
"""

from __future__ import annotations

import hashlib
import hmac
import os
import re
import secrets
from pathlib import Path

import pandas as pd
import yaml

import de_utils as U


# --------------------------------------------------------------------------- key
def pseudo_key(secrets_dir: Path) -> bytes:
    """The key comes from the environment. Locally, a random one is created outside git, once."""
    env = os.environ.get("QAFILA_PSEUDO_KEY")
    if env:
        return env.encode()
    path = Path(secrets_dir) / "pseudo_key"
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(secrets.token_hex(32), encoding="utf-8")
        path.chmod(0o600)
    return path.read_text(encoding="utf-8").strip().encode()


def pseudonymize(value: str, key: bytes) -> str:
    return hmac.new(key, value.encode(), hashlib.sha256).hexdigest()[:20]


# --------------------------------------------------------------------------- ingestion boundary
def load_customers(db, vault, secrets_dir) -> dict:
    """Split the app's customers: identifiers to the vault, a token and safe columns to raw."""
    key = pseudo_key(secrets_dir)
    source = (U.RAW / "qafila_oltp.sqlite").as_posix()
    con = U.connect(db)
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS raw")
        con.execute("INSTALL sqlite; LOAD sqlite;")
        con.execute(f"ATTACH '{source}' AS app (TYPE sqlite, READ_ONLY)")
        df = con.execute("SELECT * FROM app.customers").df()
        df["person_token"] = df["national_id"].map(lambda v: pseudonymize(v, key) if isinstance(v, str) else None)
        df["email_domain"] = df["email"].str.strip().str.lower().str.split("@").str[-1]
        safe = df[["customer_id", "person_token", "email_domain", "city", "district", "created_at", "updated_at", "is_deleted"]]
        con.register("safe", safe)
        con.execute("CREATE OR REPLACE TABLE raw.customers AS SELECT *, now()::TIMESTAMP AS _loaded_at FROM safe")
        rows = con.execute("SELECT count(*) FROM raw.customers").fetchall()[0][0]
    finally:
        con.close()
    v = U.connect(vault)
    try:
        identifiers = df[["customer_id", "person_token", "full_name", "email", "mobile", "national_id", "iban"]]
        v.register("identifiers", identifiers)
        v.execute("CREATE OR REPLACE TABLE customers_pii AS SELECT * FROM identifiers")
        v.execute("""CREATE OR REPLACE MACRO mask_mobile(m) AS
                     CASE WHEN m IS NULL THEN NULL ELSE '+9665*****' || right(regexp_replace(m, '\\D', '', 'g'), 2) END""")
        v.execute("CREATE SCHEMA IF NOT EXISTS support")
        v.execute("""CREATE OR REPLACE VIEW support.customers AS
                     SELECT customer_id, full_name, email, mask_mobile(mobile) AS mobile FROM customers_pii""")
    finally:
        v.close()
    return {"rows": rows, "people": int(df["person_token"].nunique())}


# --------------------------------------------------------------------------- role views
def build_views(db) -> dict:
    """Analysts read only their schema: no raw, staging or quarantine, and no location of a delivery."""
    con = U.connect(db)
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS analyst")
        con.execute("""CREATE OR REPLACE VIEW analyst.customers AS
                       SELECT customer_id, is_duplicate_account, city, signup_date FROM marts.dim_customers""")
        con.execute("""CREATE OR REPLACE VIEW analyst.orders AS
                       SELECT order_id, order_date, customer_id, store_id, status, payment_method, lines, units,
                              items_amount, delivery_fee, order_total, paid_amount, delivery_minutes, within_sla
                       FROM marts.fct_orders""")
        con.execute("CREATE OR REPLACE VIEW analyst.daily_city_sales AS SELECT * FROM marts.mart_daily_city_sales")
        con.execute("CREATE OR REPLACE VIEW analyst.ops_daily AS SELECT * FROM marts.mart_ops_daily")
        return {"views": 4}
    finally:
        con.close()


def load_catalog(db, path: Path) -> int:
    """Publish the column classification as a table, so tools and people read the same labels."""
    rows = yaml.safe_load(Path(path).read_text(encoding="utf-8"))["columns"]
    catalog = pd.DataFrame(rows)
    con = U.connect(db)
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS ops")
        con.register("catalog_df", catalog)
        con.execute("CREATE OR REPLACE TABLE ops.data_catalog AS SELECT * FROM catalog_df")
    finally:
        con.close()
    return len(rows)


# --------------------------------------------------------------------------- discovery (Lab 4.4)
ID_PATTERN = r"\b[12]\d{9}\b"


def is_saudi_id(s: str) -> bool:
    total = 0
    for i, ch in enumerate(reversed(s)):
        d = int(ch) * (2 if i % 2 == 1 else 1)
        total += d - 9 if d > 9 else d
    return len(s) == 10 and s[0] in "12" and total % 10 == 0


def is_saudi_iban(s: str) -> bool:
    return int("".join(str(int(c, 36)) for c in s[4:] + s[:4])) % 97 == 1


DETECTORS = {
    "national_id": (ID_PATTERN, is_saudi_id),
    "iban": (r"\bSA\d{22}\b", is_saudi_iban),
    "mobile": (r"(?<![\w+])(?:\+?966|0)\s?5\d(?:\s?\d){7}(?!\w)", lambda s: True),   # (?!\w): a hex token isn't a number
    "email": (r"[\w.+-]+@[\w-]+\.[\w.]+", lambda s: True),
}


def find_pii(text: str) -> list[str]:
    return [kind for kind, (pattern, confirm) in DETECTORS.items()
            if any(confirm(m) for m in re.findall(pattern, text or ""))]


def scan(db, schemas: list[str], sample: int = 3000) -> list[dict]:
    """Scan the values of every text column, because a column name proves nothing."""
    con = U.connect(db)
    findings = []
    try:
        listed = ", ".join(f"'{s}'" for s in schemas)
        cols = con.execute(f"""SELECT table_schema, table_name, column_name FROM information_schema.columns
                               WHERE table_schema IN ({listed}) AND data_type = 'VARCHAR' ORDER BY 1, 2, 3""").fetchall()
        for schema, table, col in cols:
            values = [v for (v,) in con.execute(f"""SELECT DISTINCT "{col}"::VARCHAR FROM "{schema}"."{table}"
                                                    WHERE "{col}" IS NOT NULL LIMIT {sample}""").fetchall()]
            kinds = sorted({k for v in values for k in find_pii(v)})
            if kinds:
                findings.append({"table": f"{schema}.{table}", "column": col, "pii": ", ".join(kinds)})
    finally:
        con.close()
    return findings
