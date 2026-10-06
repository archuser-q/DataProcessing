import argparse

from pipeline.config import get_settings
from pipeline.extract import SOURCE_TABLES, run_extract
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
    print(f"  Tổng doanh thu đơn hoàn thành: {manifest['total_revenue']:,} đ".replace(",", "."))
    print(f"Đã lưu vào {get_settings().transformed_dir.resolve()}")

def main() -> None:
    parser = argparse.ArgumentParser(prog="python -m pipeline", description="ETL pipeline cho THỢ NHANH")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("check", help="Kiểm tra kết nối và đếm số dòng các bảng nguồn")
    sub.add_parser("extract", help="Chụp toàn bộ bảng nguồn ra file Parquet (tầng raw)")
    transform = sub.add_parser("transform", help="Biến đổi dữ liệu raw thành mô hình hình sao")
    transform.add_argument("--run", help="Mã lần extract cần biến đổi (mặc định: lần mới nhất)")
    args = parser.parse_args()

    if args.command == "check":
        cmd_check()
    elif args.command == "extract":
        cmd_extract()
    elif args.command == "transform":
        cmd_transform(args.run)


if __name__ == "__main__":
    main()