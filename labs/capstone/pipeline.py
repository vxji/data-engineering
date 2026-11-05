"""Qafila Lakehouse: one Dagster code location, from raw sources to governed marts.

    python capstone/pipeline.py                    # one command, end to end (run from labs/)
    python capstone/pipeline.py --as-of 2026-09-27T00:00:00.000Z     # replay the platform at a date
    python capstone/prove_rerun.py                 # live proof that a re-run changes nothing
    dagster dev -f capstone/pipeline.py            # the UI: lineage, runs, checks, schedules

Sources -> raw (DuckDB) -> dbt (staging, marts, quarantine) -> role views -> gold tables on Iceberg.
Identifiers go to a separate vault file at the ingestion boundary.
"""

import argparse
import json
import os
import shutil
import sys
import warnings
from pathlib import Path

warnings.filterwarnings("ignore")
HERE = Path(__file__).resolve().parent
LABS = HERE.parent
sys.path[:0] = [str(LABS), str(HERE)]

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the Qafila Lakehouse once.")
    parser.add_argument("--as-of", help="replay the sources as they were at this ISO time")
    parser.add_argument("--home", help="folder for this run's warehouse, vault and lakehouse")
    parser.add_argument("--reset", action="store_true", help="delete that folder first")
    parser.add_argument("--fault", choices=["bad-status"], help="inject a contract violation, for the demo")
    ARGS = parser.parse_args()
    if ARGS.as_of:
        os.environ["QAFILA_AS_OF"] = ARGS.as_of
    if ARGS.home:
        os.environ["QAFILA_HOME"] = str(Path(ARGS.home).resolve())
    if ARGS.fault:
        os.environ["QAFILA_FAULT"] = ARGS.fault

import dagster as dg  # noqa: E402
import yaml  # noqa: E402
from dagster_dbt import DbtCliResource, dbt_assets  # noqa: E402

import de_utils as U  # noqa: E402
from governance import privacy  # noqa: E402
from ingest import incremental  # noqa: E402

HOME = Path(os.environ.get("QAFILA_HOME", U.ARTIFACTS / "capstone"))
if __name__ == "__main__" and ARGS.reset:
    shutil.rmtree(HOME, ignore_errors=True)
HOME.mkdir(parents=True, exist_ok=True)
DB = HOME / "qafila.duckdb"
VAULT = HOME / "vault.duckdb"
LAKE = HOME / "lakehouse"
SECRETS = HOME / ".secrets"
DBT_DIR = HERE / "dbt_project"
CONTRACT = HERE / "contracts" / "payments.odcs.yaml"
CATALOG = HERE / "governance" / "catalog.yaml"
os.environ["QAFILA_DUCKDB"] = DB.as_posix()          # dbt's profiles.yml reads this


def as_of() -> str | None:
    return os.environ.get("QAFILA_AS_OF") or None


def sql(query: str, params: list | None = None):
    con = U.connect(DB)
    try:
        return con.execute(query, params or []).fetchall()
    finally:
        con.close()


# ---------------------------------------------------------------------------
# 1. Ingestion (asset keys match the dbt sources: raw.<table>)
# ---------------------------------------------------------------------------
RAW = U.RAW.as_posix()
SNAPSHOT_TABLES = ["stores", "products", "orders", "order_items"]


@dg.asset(key=["raw", "customers"], group_name="ingest", kinds={"sqlite", "duckdb"},
          description="App customers. Full snapshot, so hard deletes disappear. Identifiers go to the vault.")
def raw_customers():
    return dg.MaterializeResult(metadata=privacy.load_customers(DB, VAULT, SECRETS))


def _load_snapshot(table: str) -> int:
    con = U.connect(DB)
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS raw")
        con.execute("INSTALL sqlite; LOAD sqlite;")
        con.execute(f"ATTACH '{RAW}/qafila_oltp.sqlite' AS app (TYPE sqlite, READ_ONLY)")
        con.execute(f"CREATE OR REPLACE TABLE raw.{table} AS SELECT *, now()::TIMESTAMP AS _loaded_at FROM app.{table}")
        return con.execute(f"SELECT count(*) FROM raw.{table}").fetchall()[0][0]
    finally:
        con.close()


def _make_snapshot_asset(table: str):
    @dg.asset(key=["raw", table], group_name="ingest", kinds={"sqlite", "duckdb"}, description=f"App database snapshot: {table}")
    def _asset():
        return dg.MaterializeResult(metadata={"rows": _load_snapshot(table)})
    return _asset


snapshot_assets = [_make_snapshot_asset(t) for t in SNAPSHOT_TABLES]


@dg.asset(key=["raw", "payments"], group_name="ingest", kinds={"api", "duckdb"},
          description="Payment provider API. Incremental: upsert on payment_id, cursor on updated_at.")
def raw_payments():
    return dg.MaterializeResult(metadata=incremental.load_payments(DB, as_of(), os.environ.get("QAFILA_FAULT")))


@dg.asset(key=["raw", "delivery_events"], group_name="ingest", kinds={"duckdb"},
          description="Courier events. Incremental: append by ingest_time, keyed on (event_id, ingest_time).")
def raw_delivery_events():
    return dg.MaterializeResult(metadata=incremental.load_events(DB, as_of()))


FILE_SOURCES = {
    "supplier_prices": f"read_csv('{RAW}/suppliers/*.csv', union_by_name=true, all_varchar=true, filename=true)",
    "reviews": f"read_csv('{RAW}/reviews.csv')",
}


def _make_file_asset(table: str, reader: str):
    @dg.asset(key=["raw", table], group_name="ingest", kinds={"csv", "duckdb"}, description=f"File drop: {table}")
    def _asset():
        sql("CREATE SCHEMA IF NOT EXISTS raw")
        sql(f"CREATE OR REPLACE TABLE raw.{table} AS SELECT * FROM {reader}")
        return dg.MaterializeResult(metadata={"rows": sql(f"SELECT count(*) FROM raw.{table}")[0][0]})
    return _asset


file_assets = [_make_file_asset(t, r) for t, r in FILE_SOURCES.items()]


# ---------------------------------------------------------------------------
# 2. Contract on the payments feed: a broken rule stops everything downstream
# ---------------------------------------------------------------------------
def evaluate_contract() -> tuple[list[str], list[str], int]:
    """Returns (blocking failures, rows sent to quarantine, rules checked)."""
    contract = yaml.safe_load(CONTRACT.read_text(encoding="utf-8"))
    obj = contract["schema"][0]
    total = sql("SELECT count(*) FROM raw.payments")[0][0]
    blocking, quarantined, checked = [], [], 0
    rules = [(None, r) for r in obj.get("quality", [])] + [(p["name"], r) for p in obj["properties"] for r in p.get("quality", [])]
    for col, rule in rules:
        if rule.get("type") == "sql":
            value = sql(rule["query"].replace("{object}", "raw.payments"))[0][0]
        elif rule["metric"] == "rowCount":
            value = total
        elif rule["metric"] == "duplicateValues":
            cols = ", ".join(rule["arguments"]["properties"])
            value = sql(f"SELECT count(*) - count(DISTINCT ({cols})) FROM raw.payments")[0][0]
        elif rule["metric"] == "invalidValues" and "validValues" in rule["arguments"]:
            allowed = ", ".join(f"'{v}'" for v in rule["arguments"]["validValues"])
            value = sql(f"SELECT count(*) FROM raw.payments WHERE {col} IS NOT NULL AND {col} NOT IN ({allowed})")[0][0]
        elif rule["metric"] == "invalidValues":
            value = sql(f"SELECT count(*) FROM raw.payments WHERE {col} IS NOT NULL AND NOT regexp_full_match({col}, ?)",
                        [rule["arguments"]["pattern"]])[0][0]
        else:
            continue
        checked += 1
        op, target = next((k, v) for k, v in rule.items() if k.startswith("must"))
        ok = {"mustBe": value == target, "mustBeGreaterThan": value > target, "mustBeLessThan": value < target}[op]
        if ok:
            continue
        name = rule.get("id", col)
        custom = {c["property"]: c["value"] for c in rule.get("customProperties", [])}
        if custom.get("onFail") == "quarantine" and value / max(total, 1) <= custom.get("maxShare", 0):
            quarantined.append(f"{name}: {value} rows")
        else:
            blocking.append(f"{name}: {value} ({op} {target})")
    return blocking, quarantined, checked


@dg.asset_check(asset=dg.AssetKey(["raw", "payments"]), blocking=True, description="ODCS contract: capstone/contracts/payments.odcs.yaml")
def payments_contract():
    blocking, quarantined, checked = evaluate_contract()
    return dg.AssetCheckResult(passed=not blocking, metadata={
        "rules_checked": checked, "blocking_failures": "; ".join(blocking) or "none", "sent_to_quarantine": "; ".join(quarantined) or "none"})


@dg.asset_check(asset=dg.AssetKey(["raw", "payments"]), description="Freshness: the feed is at most 1 day behind the courier events")
def payments_freshness():
    lag = sql("""SELECT date_diff('hour', (SELECT max(updated_at::TIMESTAMPTZ) FROM raw.payments),
                                 (SELECT max(ingest_time::TIMESTAMPTZ) FROM raw.delivery_events))""")[0][0]
    return dg.AssetCheckResult(passed=lag is not None and lag <= 24, severity=dg.AssetCheckSeverity.WARN, metadata={"lag_hours": lag})


# ---------------------------------------------------------------------------
# 3. dbt: staging, intermediate, marts, quarantine (every dbt test becomes an asset check)
# ---------------------------------------------------------------------------
U.run_dbt(["parse"], project_dir=DBT_DIR, db_path=DB, quiet=True)
MANIFEST = DBT_DIR / "target" / "manifest.json"
dbt_resource = DbtCliResource(project_dir=str(DBT_DIR), profiles_dir=str(DBT_DIR))


@dbt_assets(manifest=MANIFEST)
def qafila_dbt(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


@dg.asset_check(asset=dg.AssetKey(["marts", "fct_orders"]), description="Volume: no day below 70% of its weekday's 4-week median")
def orders_volume():
    rows = sql("""
        WITH d AS (SELECT order_date, count(*)::DOUBLE AS n FROM marts.fct_orders GROUP BY 1)
        SELECT order_date, n,
               (SELECT median(n) FROM d d2 WHERE d2.order_date < d.order_date AND d2.order_date >= d.order_date - 28
                  AND dayofweek(d2.order_date) = dayofweek(d.order_date)) AS expected
        FROM d ORDER BY order_date""")
    low = [str(day) for day, n, exp in rows if exp and n < 0.7 * exp]
    return dg.AssetCheckResult(passed=not low, severity=dg.AssetCheckSeverity.WARN,
                               metadata={"days_checked": len(rows), "low_days": ", ".join(low) or "none"})


# ---------------------------------------------------------------------------
# 4. Governance: role views, the column catalog, and a scan of everything people can read
# ---------------------------------------------------------------------------
@dg.asset(key=["ops", "governed_views"], group_name="govern", kinds={"duckdb"},
          deps=[dg.AssetKey(["marts", "fct_orders"]), dg.AssetKey(["marts", "dim_customers"]),
                dg.AssetKey(["marts", "mart_daily_city_sales"]), dg.AssetKey(["marts", "mart_ops_daily"])],
          description="Analyst views and the published column catalog.")
def governed_views():
    views = privacy.build_views(DB)
    return dg.MaterializeResult(metadata={**views, "catalog_columns": privacy.load_catalog(DB, CATALOG)})


@dg.asset_check(asset=dg.AssetKey(["ops", "governed_views"]), blocking=True, description="No personal data in anything analysts can read")
def no_pii_in_served_data():
    findings = privacy.scan(DB, ["marts", "analyst"])
    return dg.AssetCheckResult(passed=not findings, metadata={
        "columns_with_pii": json.dumps(findings) if findings else "none"})


@dg.asset_check(asset=dg.AssetKey(["raw", "customers"]), blocking=True, description="No identifiers in the warehouse: the vault boundary held")
def no_identifiers_in_warehouse():
    findings = [f for f in privacy.scan(DB, ["raw"]) if f["table"] == "raw.customers"]
    return dg.AssetCheckResult(passed=not findings, metadata={"columns_with_pii": json.dumps(findings) if findings else "none"})


# ---------------------------------------------------------------------------
# 5. Serve: publish gold to Iceberg so any engine can read it
# ---------------------------------------------------------------------------
def _publish(name: str, query: str) -> dg.MaterializeResult:
    con = U.connect(DB)
    try:
        data = con.sql(query).to_arrow_table()
    finally:
        con.close()
    catalog = U.get_catalog(LAKE)
    catalog.create_namespace_if_not_exists("gold")
    if catalog.table_exists(f"gold.{name}"):
        table = catalog.load_table(f"gold.{name}")
        table.overwrite(data)
    else:
        table = catalog.create_table(f"gold.{name}", schema=data.schema)
        table.append(data)
    return dg.MaterializeResult(metadata={"rows": data.num_rows, "snapshots": len(table.snapshots())})


@dg.asset(key=["gold", "daily_city_sales"], group_name="publish", kinds={"iceberg"}, deps=[governed_views],
          description="The sales dashboard mart as an Iceberg table.")
def gold_daily_city_sales():
    return _publish("daily_city_sales", "SELECT * FROM analyst.daily_city_sales ORDER BY order_date, city")


@dg.asset(key=["gold", "ops_daily"], group_name="publish", kinds={"iceberg"}, deps=[governed_views],
          description="The operations view as an Iceberg table.")
def gold_ops_daily():
    return _publish("ops_daily", "SELECT * FROM analyst.ops_daily ORDER BY delivery_date, city")


# ---------------------------------------------------------------------------
# 6. The code location
# ---------------------------------------------------------------------------
ASSETS = [raw_customers, *snapshot_assets, raw_payments, raw_delivery_events, *file_assets, qafila_dbt,
          governed_views, gold_daily_city_sales, gold_ops_daily]
CHECKS = [payments_contract, payments_freshness, orders_volume, no_pii_in_served_data, no_identifiers_in_warehouse]
everything = dg.define_asset_job("refresh_all", selection=dg.AssetSelection.all())
defs = dg.Definitions(
    assets=ASSETS,
    asset_checks=CHECKS,
    jobs=[everything],
    schedules=[dg.ScheduleDefinition(job=everything, cron_schedule="30 1 * * *", execution_timezone="Asia/Riyadh")],
    resources={"dbt": dbt_resource},
)


def write_results(result) -> dict:
    checks = result.get_asset_check_evaluations()
    rows = [{"asset": c.asset_key.to_user_string(), "check": c.check_name, "passed": bool(c.passed),
             "severity": c.severity.value, "failed_rows": (c.metadata["dagster_dbt/failed_row_count"].value
                                                          if "dagster_dbt/failed_row_count" in c.metadata else None)}
            for c in checks]
    summary = {"success": bool(result.success), "as_of": as_of(), "checks": len(rows),
               "passed": sum(r["passed"] for r in rows), "not_passed": [r for r in rows if not r["passed"]]}
    (HOME / "test_results.json").write_text(json.dumps({"summary": summary, "checks": rows}, indent=1), encoding="utf-8")
    return summary


if __name__ == "__main__":
    sys.stdout.reconfigure(errors="replace")  # consoles with an Arabic code page can't print every character
    U.ensure_data()
    result = dg.materialize([*ASSETS, *CHECKS], resources={"dbt": dbt_resource}, raise_on_error=False,
                            run_config={"loggers": {"console": {"config": {"log_level": "ERROR"}}}})
    mats = result.get_asset_materialization_events()
    summary = write_results(result)
    print(f"\nrun {'succeeded' if result.success else 'STOPPED'}: {len(mats)} assets built, "
          f"{summary['passed']} of {summary['checks']} checks passed")
    for c in result.get_asset_check_evaluations():
        if not c.passed:
            meta = {k: v.value for k, v in c.metadata.items()}
            detail = meta.get("blocking_failures") or meta.get("failures") or meta.get("low_days") or meta.get("columns_with_pii") \
                or f"{meta.get('dagster_dbt/failed_row_count', '?')} failing rows"
            print(f"  {c.severity.value:7} {c.asset_key.to_user_string()} · {c.check_name}: {detail}")
    print(f"warehouse: {DB}\nvault: {VAULT}\nlakehouse: {LAKE}")
    sys.exit(0 if result.success else 1)
