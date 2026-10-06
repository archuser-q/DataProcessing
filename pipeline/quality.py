import json
import os
import time
from datetime import datetime, timedelta

os.environ.setdefault("GX_ANALYTICS_ENABLED", "False")

import duckdb  # noqa: E402
import great_expectations as gx  # noqa: E402
import pandas as pd  # noqa: E402
from great_expectations.data_context.types.base import ProgressBarsConfig  # noqa: E402

from pipeline.config import get_settings  # noqa: E402
from pipeline.extract import VN_TZ  # noqa: E402
from pipeline.transform import latest_run_id  # noqa: E402

E = gx.expectations
ORDER_STATUSES = ["pending", "matched", "accepted", "on_the_way", "arrived", "in_progress", "completed", "cancelled"]
PAYMENT_STATUSES = ["pending", "success", "failed", "refunded"]
FRESHNESS_DAYS = 7


def _error(expectation):
    expectation.meta = {"severity": "error"}
    return expectation


def _warning(expectation):
    expectation.meta = {"severity": "warning"}
    return expectation


def build_suites(frames: dict[str, pd.DataFrame]) -> dict[str, list]:
    keys = {
        "date": frames["dim_date"]["date_key"].tolist(),
        "service": frames["dim_service"]["service_key"].tolist(),
        "worker": frames["dim_worker"]["worker_key"].tolist(),
        "customer": frames["dim_customer"]["customer_key"].tolist(),
    }
    fresh_after = datetime.now(VN_TZ).replace(tzinfo=None) - timedelta(days=FRESHNESS_DAYS)

    return {
        "fact_orders": [
            _error(E.ExpectTableRowCountToBeBetween(min_value=1)),
            _error(E.ExpectColumnValuesToNotBeNull(column="order_id")),
            _error(E.ExpectColumnValuesToBeUnique(column="order_id")),
            _error(E.ExpectColumnValuesToBeInSet(column="status", value_set=ORDER_STATUSES)),
            _error(E.ExpectColumnValuesToBeInSet(column="date_key", value_set=keys["date"])),
            _error(E.ExpectColumnValuesToBeInSet(column="service_key", value_set=keys["service"])),
            _error(E.ExpectColumnValuesToBeInSet(column="customer_key", value_set=keys["customer"])),
            _error(E.ExpectColumnValuesToBeInSet(column="worker_key", value_set=keys["worker"])),
            _error(E.ExpectColumnValuesToBeBetween(column="estimated_price", min_value=0)),
            _error(
                E.ExpectColumnValuesToNotBeNull(
                    column="final_price", row_condition="is_completed == True", condition_parser="pandas"
                )
            ),
            _error(
                E.ExpectColumnPairValuesAToBeGreaterThanB(
                    column_A="final_price",
                    column_B="paid_amount",
                    or_equal=True,
                    ignore_row_if="either_value_is_missing",
                )
            ),
            _error(E.ExpectColumnValuesToBeBetween(column="match_minutes", min_value=0)),
            _error(E.ExpectColumnValuesToBeBetween(column="rating", min_value=1, max_value=5)),
            _error(E.ExpectColumnValuesToBeInSet(column="payment_status", value_set=PAYMENT_STATUSES)),
            _warning(E.ExpectColumnMaxToBeBetween(column="created_at", min_value=fresh_after)),
        ],
        "dim_service": [
            _error(E.ExpectColumnValuesToBeUnique(column="service_key")),
            _error(E.ExpectColumnValuesToNotBeNull(column="service_name")),
            _error(E.ExpectColumnValuesToBeBetween(column="base_price", min_value=1)),
        ],
        "dim_worker": [
            _error(E.ExpectColumnValuesToBeUnique(column="worker_key")),
            _error(E.ExpectColumnValuesToBeBetween(column="trust_score", min_value=0, max_value=1)),
            _error(
                E.ExpectColumnValuesToBeInSet(
                    column="verification_status", value_set=["pending", "approved", "rejected"]
                )
            ),
        ],
        "dim_customer": [
            _error(E.ExpectColumnValuesToBeUnique(column="customer_key")),
        ],
        "dim_date": [
            _error(E.ExpectColumnValuesToBeUnique(column="date_key")),
            _error(E.ExpectColumnValuesToNotBeNull(column="full_date")),
        ],
        "agg_daily_service": [
            _error(E.ExpectCompoundColumnsToBeUnique(column_list=["date_key", "service_key"])),
            _error(E.ExpectColumnValuesToBeBetween(column="revenue", min_value=0)),
            _error(
                E.ExpectColumnPairValuesAToBeGreaterThanB(
                    column_A="orders_created", column_B="orders_completed", or_equal=True
                )
            ),
        ],
    }


def _plain(value) -> str:
    """Đổi giá trị numpy/pandas sang chữ dễ đọc trong báo cáo."""
    if isinstance(value, (tuple, list)):
        return "(" + ", ".join(_plain(v) for v in value) + ")"
    if hasattr(value, "item"):
        value = value.item()
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    return str(value)


def _describe(result) -> dict:
    config = result.expectation_config
    kwargs = {k: v for k, v in config.kwargs.items() if k not in ("batch_id", "value_set")}
    if "value_set" in config.kwargs:
        kwargs["value_set_size"] = len(config.kwargs["value_set"])
    details = result.result or {}
    return {
        "expectation": config.type,
        "kwargs": {k: str(v) for k, v in kwargs.items()},
        "severity": (config.meta or {}).get("severity", "error"),
        "success": bool(result.success),
        "unexpected_count": details.get("unexpected_count"),
        "sample": [_plain(v) for v in (details.get("partial_unexpected_list") or [])[:5]],
        "observed_value": str(details["observed_value"]) if "observed_value" in details else None,
    }


def run_quality(run_id: str | None = None) -> dict:
    settings = get_settings()
    run_id = run_id or latest_run_id()
    source_dir = settings.transformed_dir / f"run={run_id}"
    if not (source_dir / "_manifest.json").exists():
        raise RuntimeError(f"Lần chạy {run_id} chưa được transform, hãy chạy: python -m pipeline transform")

    started = time.perf_counter()
    frames = {
        table: duckdb.sql(f"SELECT * FROM read_parquet('{(source_dir / f'{table}.parquet').as_posix()}')").df()
        for table in ["fact_orders", "dim_service", "dim_worker", "dim_customer", "dim_date", "agg_daily_service"]
    }

    context = gx.get_context(mode="ephemeral")
    context.variables.progress_bars = ProgressBarsConfig(globally=False)
    datasource = context.data_sources.add_pandas("pipeline")

    tables = {}
    for table, expectations in build_suites(frames).items():
        batch = datasource.add_dataframe_asset(name=table).add_batch_definition_whole_dataframe("all")
        suite = context.suites.add(gx.ExpectationSuite(name=f"{table}_suite"))
        for expectation in expectations:
            suite.add_expectation(expectation)
        validation = context.validation_definitions.add(
            gx.ValidationDefinition(name=f"{table}_validation", data=batch, suite=suite)
        )
        result = validation.run(batch_parameters={"dataframe": frames[table]})
        tables[table] = [_describe(r) for r in result.results]

    checks = [c for results in tables.values() for c in results]
    failed_errors = [c for c in checks if not c["success"] and c["severity"] == "error"]
    failed_warnings = [c for c in checks if not c["success"] and c["severity"] == "warning"]
    report = {
        "run_id": run_id,
        "validated_at": datetime.now(VN_TZ).isoformat(timespec="seconds"),
        "duration_seconds": round(time.perf_counter() - started, 3),
        "success": not failed_errors,
        "total_checks": len(checks),
        "failed_errors": len(failed_errors),
        "failed_warnings": len(failed_warnings),
        "tables": tables,
    }
    out_dir = settings.quality_dir / f"run={run_id}"
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return report