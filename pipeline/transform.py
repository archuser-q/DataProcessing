import json
import time
from datetime import datetime
from pathlib import Path

import duckdb

from pipeline.config import get_settings
from pipeline.extract import SOURCE_TABLES, VN_TZ


def latest_run_id() -> str:
    runs = sorted(get_settings().raw_dir.glob("_runs/*.json"))
    if not runs:
        raise RuntimeError("Chưa có lần extract nào, hãy chạy: python -m pipeline extract")
    return runs[-1].stem


def _register_raw(con: duckdb.DuckDBPyConnection, run_id: str) -> None:
    raw_dir = get_settings().raw_dir
    for table in SOURCE_TABLES:
        file = raw_dir / table / f"run={run_id}" / f"{table}.parquet"
        if not file.exists():
            raise RuntimeError(f"Thiếu file raw của bảng {table} ở lần chạy {run_id}")
        con.execute(f"CREATE VIEW raw_{table} AS SELECT * FROM read_parquet('{file.as_posix()}')")


def _has_column(con: duckdb.DuckDBPyConnection, view: str, column: str) -> bool:
    return column in [row[0] for row in con.execute(f"DESCRIBE {view}").fetchall()]

STEPS: list[tuple[str, str]] = [
    (
        "dim_service",
        """
        SELECT
            s.id                 AS service_key,
            s.name               AS service_name,
            s.category_id,
            c.name               AS category_name,
            s.base_price,
            s.unit,
            s.is_active AND COALESCE(c.is_active, TRUE) AS is_active
        FROM raw_services s
        LEFT JOIN raw_service_categories c ON c.id = s.category_id
        """,
    ),
    (
        "dim_worker",
        """
        SELECT
            u.id                          AS worker_key,
            u.full_name,
            u.status                      AS account_status,
            w.verification_status,
            w.experience_years,
            w.service_radius_km,
            w.trust_score,
            CAST(u.created_at AS DATE)    AS registered_date
        FROM raw_users u
        JOIN raw_worker_profiles w ON w.user_id = u.id
        WHERE u.role = 'worker'
        """,
    ),
    (
        "dim_customer",
        """
        SELECT
            id                            AS customer_key,
            status                        AS account_status,
            CAST(created_at AS DATE)      AS registered_date
        FROM raw_users
        WHERE role = 'customer'
        """,
    ),
    (
        "quarantine_orders",
        """
        SELECT o.*, reason
        FROM (
            SELECT o.*,
                CASE
                    WHEN o.status = 'completed' AND o.completed_at IS NULL
                        THEN 'Đơn hoàn thành nhưng thiếu thời điểm hoàn thành'
                    WHEN o.status = 'completed' AND o.final_price IS NULL
                        THEN 'Đơn hoàn thành nhưng thiếu giá chốt'
                    WHEN o.status NOT IN ('pending', 'cancelled') AND o.worker_id IS NULL
                        THEN 'Đơn đã ghép thợ nhưng thiếu mã thợ'
                    WHEN o.accepted_at < o.created_at OR o.completed_at < o.accepted_at
                        THEN 'Thứ tự thời gian không hợp lệ'
                    WHEN COALESCE(o.final_price, o.estimated_price) < 0
                        THEN 'Giá âm'
                END AS reason
            FROM raw_orders o
        ) o
        WHERE reason IS NOT NULL
        """,
    ),
    (
        "fact_orders",
        """
        WITH payment AS (
            -- Mỗi đơn lấy một giao dịch đại diện: ưu tiên giao dịch thành công, rồi tới mới nhất
            SELECT * EXCLUDE (rn) FROM (
                SELECT p.*, ROW_NUMBER() OVER (
                    PARTITION BY order_id
                    ORDER BY (status = 'success') DESC, created_at DESC, id DESC
                ) AS rn
                FROM raw_payments p
            ) WHERE rn = 1
        ),
        review AS (
            SELECT * FROM raw_reviews WHERE {review_visible}
        )
        SELECT
            o.id                                                AS order_id,
            CAST(strftime(o.created_at, '%Y%m%d') AS INTEGER)   AS date_key,
            o.customer_id                                       AS customer_key,
            o.worker_id                                         AS worker_key,
            o.service_id                                        AS service_key,
            o.status,
            o.status = 'completed'                              AS is_completed,
            o.status = 'cancelled'                              AS is_cancelled,
            o.matching_mode,
            o.estimated_price,
            o.final_price,
            o.final_price - o.estimated_price                   AS extra_charge,
            date_diff('second', o.created_at, o.accepted_at) / 60.0    AS match_minutes,
            date_diff('second', o.accepted_at, o.completed_at) / 60.0  AS service_minutes,
            pay.method                                          AS payment_method,
            pay.status                                          AS payment_status,
            CASE WHEN pay.status = 'success' THEN pay.amount END AS paid_amount,
            e.commission_amount,
            e.net_amount                                        AS worker_net_amount,
            r.rating,
            o.created_at,
            o.accepted_at,
            o.completed_at
        FROM raw_orders o
        LEFT JOIN payment pay ON pay.order_id = o.id
        LEFT JOIN raw_worker_earnings e ON e.order_id = o.id
        LEFT JOIN review r ON r.order_id = o.id
        WHERE o.id NOT IN (SELECT id FROM quarantine_orders)
        """,
    ),
    (
        # Lịch đủ mọi ngày từ đơn đầu tiên tới đơn cuối cùng, kể cả ngày không có đơn
        "dim_date",
        """
        SELECT
            CAST(strftime(d, '%Y%m%d') AS INTEGER) AS date_key,
            CAST(d AS DATE)                        AS full_date,
            year(d)                                AS year,
            quarter(d)                             AS quarter,
            month(d)                               AS month,
            day(d)                                 AS day,
            isodow(d)                              AS day_of_week,
            isodow(d) IN (6, 7)                    AS is_weekend
        FROM range(
            (SELECT CAST(MIN(created_at) AS DATE) FROM raw_orders),
            (SELECT CAST(MAX(created_at) AS DATE) FROM raw_orders) + INTERVAL 1 DAY,
            INTERVAL 1 DAY
        ) AS t(d)
        """,
    ),
    (
        "agg_daily_service",
        """
        SELECT
            date_key,
            service_key,
            COUNT(*)                                        AS orders_created,
            COUNT(*) FILTER (WHERE is_completed)            AS orders_completed,
            COUNT(*) FILTER (WHERE is_cancelled)            AS orders_cancelled,
            COALESCE(SUM(final_price) FILTER (WHERE is_completed), 0)  AS revenue,
            COALESCE(SUM(commission_amount), 0)             AS commission,
            ROUND(AVG(match_minutes), 1)                    AS avg_match_minutes,
            ROUND(AVG(rating), 2)                           AS avg_rating
        FROM fact_orders
        GROUP BY date_key, service_key
        """,
    ),
]


def run_transform(run_id: str | None = None) -> dict:
    settings = get_settings()
    run_id = run_id or latest_run_id()
    started = time.perf_counter()
    out_dir = settings.transformed_dir / f"run={run_id}"
    out_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()  # chạy trong bộ nhớ, không cần server
    try:
        _register_raw(con, run_id)
        review_visible = "NOT is_hidden" if _has_column(con, "raw_reviews", "is_hidden") else "TRUE"

        outputs = {}
        for name, sql in STEPS:
            con.execute(f"CREATE TABLE {name} AS {sql.format(review_visible=review_visible)}")
            file = out_dir / f"{name}.parquet"
            con.execute(f"COPY {name} TO '{file.as_posix()}' (FORMAT parquet)")
            outputs[name] = con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]

        quarantine_reasons = dict(
            con.execute("SELECT reason, COUNT(*) FROM quarantine_orders GROUP BY reason").fetchall()
        )
        revenue = con.execute("SELECT COALESCE(SUM(revenue), 0) FROM agg_daily_service").fetchone()[0]
    finally:
        con.close()

    manifest = {
        "run_id": run_id,
        "transformed_at": datetime.now(VN_TZ).isoformat(timespec="seconds"),
        "duration_seconds": round(time.perf_counter() - started, 3),
        "outputs": outputs,
        "quarantine_reasons": quarantine_reasons,
        "total_revenue": int(revenue),
    }
    (out_dir / "_manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest