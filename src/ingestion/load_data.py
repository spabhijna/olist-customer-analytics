import duckdb
import os
from pathlib import Path


def load_csv_duckdb(
    con: duckdb.DuckDBPyConnection,
    csv_path: str,
    table_name: str,
    replace: bool,
) -> int:
    """Load one CSV into a table. Returns the row count."""
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    if replace:
        con.execute(f"DROP TABLE IF EXISTS {table_name}")

    con.execute(f"""
        CREATE TABLE {table_name} AS
        SELECT * FROM read_csv_auto('{csv_path}', header=True)
    """)

    return con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]


def _sanitize_table_name(name: str) -> str:
    """Convert a filename stem into a safe SQL identifier."""
    cleaned = "".join(c if c.isalnum() or c == "_" else "_" for c in name)
    if cleaned and cleaned[0].isdigit():
        cleaned = f"t_{cleaned}"
    return cleaned.lower()


class IngestionError(Exception):
    pass


def main():
    db_path = "data/processed/olist.duckdb"
    csv_dir = "data/raw"

    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    csv_files = sorted(str(p) for p in Path(csv_dir).glob("*.csv"))

    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in '{csv_dir}/'")

    print(f"Starting ingestion of {len(csv_files)} file(s) into '{db_path}'\n")

    succeeded: list[tuple[str, str, int]] = []
    failed: list[tuple[str, str]] = []

    with duckdb.connect(db_path) as con:  # ← single connection, auto-closed
        for csv_path in csv_files:
            table_name = _sanitize_table_name(Path(csv_path).stem)
            try:
                row_count = load_csv_duckdb(
                    con=con,
                    csv_path=csv_path,
                    table_name=table_name,
                    replace=True,
                )
                succeeded.append((csv_path, table_name, row_count))
                print(f"✅ Loaded {row_count} rows into '{table_name}' from {csv_path}")
            except Exception as e:
                print(f"❌ Failed to load {csv_path}: {e}")
                failed.append((csv_path, str(e)))

    # Summary
    print("\n" + "=" * 60)
    print(f"Ingestion complete: {len(succeeded)} succeeded, {len(failed)} failed")
    for path, table, rows in succeeded:
        print(f"  ✅ {path}  →  {table} ({rows} rows)")
    for path, err in failed:
        print(f"  ❌ {path}  →  {err}")
    print("=" * 60)

    if failed:
        raise IngestionError(f"{len(failed)} file(s) failed to ingest")


if __name__ == "__main__":
    try:
        main()
    except IngestionError as e:
        print(f"\n💥 {e}")
        raise SystemExit(1)