import json
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import Connection, text

from pipeline.config import get_settings
from pipeline.source import get_engine

VN_TZ = ZoneInfo("Asia/Ho_Chi_Minh")

SOURCE_TABLES: dict[str, set[str]] = {
    "users": {"password_hash", "phone", "email", "avatar_url"},
    "worker_profiles": {
        "bio",
        "current_latitude",
        "current_longitude",
        "location",
        "geohash",
        "location_updated_at",
    },
    "service_categories": {"icon_url"},
    "services": set(),
    "orders": {"address_line", "description", "image_urls"},
    "payments": {"transaction_code"},
    "worker_earnings": set(),
    "reviews": set(),
}


@dataclass
class TableResult:
    table: str
    rows: int
    columns: dict[str, str]  # tên cột -> kiểu dữ liệu
    excluded: list[str]
    file: str


def new_run_id() -> str:
    return datetime.now(VN_TZ).strftime("%Y%m%dT%H%M%S")


def _arrow_type(data_type: str, precision: int | None, scale: int | None) -> pa.DataType:
    """Đổi kiểu PostgreSQL sang kiểu Arrow/Parquet tương ứng."""
    if data_type == "bigint":
        return pa.int64()
    if data_type == "integer":
        return pa.int32()
    if data_type == "smallint":
        return pa.int16()
    if data_type == "numeric":
        return pa.decimal128(precision or 38, scale or 0)
    if data_type in ("double precision", "real"):
        return pa.float64()
    if data_type == "boolean":
        return pa.bool_()
    if data_type == "timestamp without time zone":
        return pa.timestamp("us")  # giờ Việt Nam, giống quy ước của backend
    if data_type == "date":
        return pa.date32()
    return pa.string()


def _columns(conn: Connection, table: str) -> list[tuple[str, pa.DataType]]:
    rows = conn.execute(
        text(
            """
            SELECT column_name, data_type, numeric_precision, numeric_scale
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = :table
            ORDER BY ordinal_position
            """
        ),
        {"table": table},
    ).all()
    return [(name, _arrow_type(dtype, precision, scale)) for name, dtype, precision, scale in rows]


def _extract_table(conn: Connection, table: str, excluded: set[str], out_dir: Path) -> TableResult:
    all_columns = _columns(conn, table)
    if not all_columns:
        raise RuntimeError(f"Không tìm thấy bảng nguồn: {table}")
    keep = [(name, dtype) for name, dtype in all_columns if name not in excluded]
    column_sql = ", ".join(f'"{name}"' for name, _ in keep)

    rows = conn.execute(text(f'SELECT {column_sql} FROM "{table}" ORDER BY 1')).all()
    schema = pa.schema([pa.field(name, dtype) for name, dtype in keep])
    arrow_table = pa.Table.from_pylist([dict(row._mapping) for row in rows], schema=schema)

    out_dir.mkdir(parents=True, exist_ok=True)
    file = out_dir / f"{table}.parquet"
    pq.write_table(arrow_table, file)
    return TableResult(
        table=table,
        rows=arrow_table.num_rows,
        columns={name: str(dtype) for name, dtype in keep},
        excluded=sorted(name for name, _ in all_columns if name in excluded),
        file=str(file),
    )


def run_extract(run_id: str | None = None) -> dict:
    settings = get_settings()
    run_id = run_id or new_run_id()
    started = time.perf_counter()
    results: list[TableResult] = []

    engine = get_engine().execution_options(isolation_level="REPEATABLE READ")
    with engine.connect() as conn, conn.begin():
        conn.execute(text("SET TRANSACTION READ ONLY"))
        for table, excluded in SOURCE_TABLES.items():
            out_dir = settings.raw_dir / table / f"run={run_id}"
            results.append(_extract_table(conn, table, excluded, out_dir))

    manifest = {
        "run_id": run_id,
        "extracted_at": datetime.now(VN_TZ).isoformat(timespec="seconds"),
        "duration_seconds": round(time.perf_counter() - started, 3),
        "tables": [asdict(r) for r in results],
    }
    runs_dir = settings.raw_dir / "_runs"
    runs_dir.mkdir(parents=True, exist_ok=True)
    (runs_dir / f"{run_id}.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest