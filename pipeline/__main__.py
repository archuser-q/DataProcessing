import argparse

from pipeline.config import get_settings
from pipeline.extract import SOURCE_TABLES, run_extract
from pipeline.load import run_load
from pipeline.source import table_row_counts
from pipeline.transform import run_transform


def cmd_check() -> None:
    settings = get_settings()
    print(f"Thư mục dữ liệu: {settings.data_dir.resolve()}")
    for table, count in table_row_counts(list(SOURCE_TABLES)).items():
        print(f"  {table:<20} {count:>8,} dòng")
    print("Kết nối database nguồn: OK")

def cmd_extract() -> None:
    manifest = run_extract()
    print(f"Lần chạy {manifest['run_id']} ({manifest['duration_seconds']} giây)")
    for t in manifest["tables"]:
        excluded = f"  (bỏ {', '.join(t['excluded'])})" if t["excluded"] else ""
        print(f"  {t['table']:<20} {t['rows']:>8,} dòng{excluded}")
    print(f"Đã lưu vào {get_settings().raw_dir.resolve()}")

def cmd_transform(run_id: str | None) -> None:
    manifest = run_transform(run_id)
    print(f"Biến đổi lần chạy {manifest['run_id']} ({manifest['duration_seconds']} giây)")
    for name, rows in manifest["outputs"].items():
        print(f"  {name:<20} {rows:>8,} dòng")
    for reason, count in manifest["quarantine_reasons"].items():
        print(f"  Cách ly {count} đơn: {reason}")
    money = f"{manifest['total_revenue']:,}".replace(",", ".")
    print(f"  Tổng doanh thu đơn hoàn thành: {money} đ")
    print(f"Đã lưu vào {get_settings().transformed_dir.resolve()}")

def cmd_load(run_id: str | None, force: bool) -> None:
    result = run_load(run_id, force)
    print(f"Nạp lần chạy {result['run_id']} vào kho ({result['duration_seconds']} giây)")
    for table, rows in result["tables"].items():
        print(f"  dw.{table:<20} {rows:>8,} dòng")
    print("  Doanh thu 3 tháng gần nhất:")
    for year, month, completed, revenue in result["recent_months"]:
        money = f"{int(revenue):,}".replace(",", ".")
        print(f"    {month:02d}/{year}: {completed:>4} đơn hoàn thành, {money} đ")
    print(f"Kho dữ liệu: {result['warehouse']}")

def main() -> None:
    parser = argparse.ArgumentParser(prog="python -m pipeline", description="ETL pipeline cho THỢ NHANH")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("check", help="Kiểm tra kết nối và đếm số dòng các bảng nguồn")
    sub.add_parser("extract", help="Chụp toàn bộ bảng nguồn ra file Parquet (tầng raw)")
    transform = sub.add_parser("transform", help="Biến đổi dữ liệu raw thành mô hình hình sao")
    load = sub.add_parser("load", help="Nạp kết quả transform vào kho DuckDB")
    load.add_argument("--run", help="Mã lần chạy cần nạp (mặc định: lần mới nhất)")
    load.add_argument("--force", action="store_true", help="Cho phép nạp một lần chạy cũ hơn lần đã nạp")
    args = parser.parse_args()

    if args.command == "check":
        cmd_check()
    elif args.command == "extract":
        cmd_extract()
    elif args.command == "transform":
        cmd_transform(args.run)
    elif args.command == "load":
        cmd_load(args.run, args.force)


if __name__ == "__main__":
    main()