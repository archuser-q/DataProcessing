import argparse

from pipeline.config import get_settings
from pipeline.source import table_row_counts

SOURCE_TABLES = [
    "users",
    "worker_profiles",
    "service_categories",
    "services",
    "orders",
    "payments",
    "worker_earnings",
    "reviews",
]


def cmd_check() -> None:
    settings = get_settings()
    print(f"Thư mục dữ liệu: {settings.data_dir.resolve()}")
    for table, count in table_row_counts(SOURCE_TABLES).items():
        print(f"  {table:<20} {count:>8,} dòng")
    print("Kết nối database nguồn: OK")


def main() -> None:
    parser = argparse.ArgumentParser(prog="python -m pipeline", description="ETL pipeline cho THỢ NHANH")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("check", help="Kiểm tra kết nối và đếm số dòng các bảng nguồn")
    args = parser.parse_args()

    if args.command == "check":
        cmd_check()


if __name__ == "__main__":
    main()