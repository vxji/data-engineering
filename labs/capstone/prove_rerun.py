"""Live proofs for the demo (run from labs/).

    python capstone/prove_rerun.py            # a re-run changes nothing; a later run only adds what's new
    python capstone/prove_rerun.py --block    # a contract violation stops the run; the marts keep their numbers
"""

import argparse
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import duckdb
import pandas as pd

HERE = Path(__file__).resolve().parent
PIPELINE = HERE / "pipeline.py"
DAY_ONE, DAY_THREE = "2026-09-27T00:00:00.000Z", "2026-09-29T00:00:00.000Z"
TABLES = ["raw.payments", "raw.delivery_events", "staging.stg_payments", "quarantine.qrn_payments",
          "marts.fct_order_items", "marts.fct_orders", "marts.mart_daily_city_sales", "marts.mart_ops_daily"]


def run(home: Path, as_of: str, *extra: str) -> tuple[int, str]:
    done = subprocess.run([sys.executable, str(PIPELINE), "--home", str(home), "--as-of", as_of, *extra],
                          capture_output=True, text=True, errors="replace")
    summary = [line for line in done.stdout.splitlines() if line.startswith(("run ", "  ERROR"))]
    print(f"  as of {as_of[:10]}: " + " | ".join(s.strip() for s in summary))
    return done.returncode, done.stdout


def fingerprint(home: Path) -> pd.DataFrame:
    """Row count and an order-independent hash of every row, ignoring load timestamps."""
    con = duckdb.connect(str(home / "qafila.duckdb"), read_only=True)
    rows = []
    for table in TABLES:
        cols = [c[0] for c in con.execute(f"DESCRIBE {table}").fetchall() if c[0] != "_loaded_at"]
        n, digest = con.execute(f"SELECT count(*), bit_xor(hash({', '.join(cols)})) FROM {table}").fetchone()
        rows.append({"table": table, "rows": n, "hash": digest})
    con.close()
    return pd.DataFrame(rows).set_index("table")


def ingest_log(home: Path) -> pd.DataFrame:
    con = duckdb.connect(str(home / "qafila.duckdb"), read_only=True)
    log = con.execute("""SELECT as_of[:10] AS as_of, source, rows_fetched AS fetched, rows_new AS new, rows_updated AS updated,
                                rows_unchanged AS unchanged FROM ops.ingest_log ORDER BY logged_at""").df()
    con.close()
    return log


def prove_rerun(home: Path) -> bool:
    print("1. First load")
    codes = [run(home, DAY_ONE)[0]]
    first = fingerprint(home)
    print("2. The scheduler runs it again, same day")
    codes.append(run(home, DAY_ONE)[0])
    second = fingerprint(home)
    print("3. Two days later: new payments, refunds of old ones, new events")
    codes.append(run(home, DAY_THREE)[0])
    third = fingerprint(home)
    table = pd.DataFrame({"first run": first["rows"], "re-run": second["rows"], "2 days later": third["rows"],
                          "re-run identical": first["hash"] == second["hash"]})
    print("\n" + table.to_string() + "\n\n" + ingest_log(home).to_string(index=False))
    ok = bool(table["re-run identical"].all()) and not any(codes)
    print(f"\n{'PASS' if ok else 'FAIL'}: the re-run left every table identical, row for row")
    return ok


def prove_block(home: Path) -> bool:
    print("1. A good run")
    run(home, DAY_ONE)
    before = fingerprint(home)
    print("2. The provider starts sending a status nobody agreed: refund_pending")
    code, output = run(home, DAY_THREE, "--fault", "bad-status")
    after = fingerprint(home)
    marts = [t for t in TABLES if t.startswith("marts.")]
    kept = bool((before.loc[marts, "hash"] == after.loc[marts, "hash"]).all())
    print("\n" + pd.DataFrame({"before": before["rows"], "after the bad feed": after["rows"]}).to_string())
    ok = code != 0 and kept and "payments_contract" in output
    print(f"\n{'PASS' if ok else 'FAIL'}: the run stopped, and the marts kept yesterday's numbers")
    return ok


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--block", action="store_true")
    args = parser.parse_args()
    home = Path(tempfile.mkdtemp(prefix="qafila_proof_"))
    try:
        ok = prove_block(home) if args.block else prove_rerun(home)
    finally:
        shutil.rmtree(home, ignore_errors=True)
    sys.exit(0 if ok else 1)
