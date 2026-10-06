import time
from datetime import datetime

import duckdb

from pipeline.config import get_settings
from pipeline.extract import VN_TZ
from pipeline.transform import latest_run_id

LOAD_TABLES: dict[str, list[str]] = {
    "dim_date": ["date_key"],
    "dim_service": ["service_key"],
    "dim_worker": ["worker_key"],
    "dim_customer": ["customer_key"],
    "fact_orders": ["order_id"],
    "agg_daily_service": ["date_key", "service_key"],
    "quarantine_orders": ["id"],
}

VIEWS = {
    "v_monthly_revenue": """
        SELECT
            d.year,
            d.month,
            SUM(a.orders_created)    AS orders_created,
            SUM(a.orders_completed)  AS orders_completed,
            SUM(a.revenue)           AS revenue,
            SUM(a.commission)        AS commission
        FROM dw.agg_daily_service a
        JOIN dw.dim_date d USING (date_key)
        GROUP BY d.year, d.month
    """,
    "v_service_performance": """
        SELECT
            s.category_name,
            s.service_name,
            COUNT(*)                                   AS orders,
            COUNT(*) FILTER (WHERE f.is_completed)     AS completed,
            ROUND(100.0 * COUNT(*) FILTER (WHERE f.is_cancelled) / COUNT(*), 1) AS cancel_rate_pct,
            ROUND(AVG(f.final_price) FILTER (WHERE f.is_completed))             AS avg_final_price,
            ROUND(AVG(f.rating), 2)                    AS avg_rating
        FROM dw.fact_orders f
        JOIN dw.dim_service s USING (service_key)
        GROUP BY s.category_name, s.service_name
    """,
}


def _ensure_audit(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS dw")
    con.execute("CREATE SCHEMA IF NOT EXISTS audit")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS audit.load_runs (
            run_id      VARCHAR,
            table_name  VARCHAR,
            row_count   BIGINT,
            loaded_at   TIMESTAMP
        )
        """
    )


def _check_primary_key(con: duckdb.DuckDBPyConnection, table: str, file: str, keys: list[str]) -> None:
    key_sql = ", ".join(keys)
    duplicates = con.execute(
        f"SELECT COUNT(*) FROM (SELECT {key_sql} FROM read_parquet('{file}') "
        f"GROUP BY {key_sql} HAVING COUNT(*) > 1)"
    ).fetchone()[0]
    if duplicates:
        raise RuntimeError(f"{table}: {duplicates} giá trị khóa ({key_sql}) bị trùng, dừng nạp")


def run_load(run_id: str | None = None, force: bool = False) -> dict:
    settings = get_settings()
    run_id = run_id or latest_run_id()
    source_dir = settings.transformed_dir / f"run={run_id}"
    if not (source_dir / "_manifest.json").exists():
        raise RuntimeError(f"Lần chạy {run_id} chưa được transform, hãy chạy: python -m pipeline transform")

    started = time.perf_counter()
    settings.warehouse_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(settings.warehouse_path))
    try:
        _ensure_audit(con)
        last_loaded = con.execute("SELECT MAX(run_id) FROM audit.load_runs").fetchone()[0]
        if last_loaded and run_id < last_loaded and not force:
            raise RuntimeError(
                f"Kho đang ở lần chạy {last_loaded}, mới hơn {run_id}. Dùng --force nếu thực sự muốn nạp lùi."
            )

        loaded_at = datetime.now(VN_TZ).replace(tzinfo=None)
        counts: dict[str, int] = {}
        con.execute("BEGIN TRANSACTION")
        try:
            for table, keys in LOAD_TABLES.items():
                file = (source_dir / f"{table}.parquet").as_posix()
                _check_primary_key(con, table, file, keys)
                con.execute(f"CREATE OR REPLACE TABLE dw.{table} AS SELECT * FROM read_parquet('{file}')")
                counts[table] = con.execute(f"SELECT COUNT(*) FROM dw.{table}").fetchone()[0]
                con.execute(
                    "INSERT INTO audit.load_runs VALUES (?, ?, ?, ?)", [run_id, table, counts[table], loaded_at]
                )
            for name, sql in VIEWS.items():
                con.execute(f"CREATE OR REPLACE VIEW dw.{name} AS {sql}")
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise

        recent = con.execute(
            "SELECT year, month, orders_completed, revenue FROM dw.v_monthly_revenue ORDER BY year DESC, month DESC LIMIT 3"
        ).fetchall()
    finally:
        con.close()

    return {
        "run_id": run_id,
        "duration_seconds": round(time.perf_counter() - started, 3),
        "tables": counts,
        "recent_months": recent,
        "warehouse": str(settings.warehouse_path.resolve()),
    }