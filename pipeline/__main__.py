import argparse
import sys

from pipeline.config import get_settings
from pipeline.extract import SOURCE_TABLES, run_extract
from pipeline.load import run_load
from pipeline.quality import run_quality
from pipeline.runner import read_history, run_pipeline
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


def cmd_validate(run_id: str | None) -> bool:
    report = run_quality(run_id)
    print(f"Kiểm tra chất lượng lần chạy {report['run_id']} ({report['duration_seconds']} giây)")
    for table, checks in report["tables"].items():
        passed = sum(c["success"] for c in checks)
        print(f"  {table:<20} {passed}/{len(checks)} quy tắc đạt")
        for c in checks:
            if not c["success"]:
                label = "LỖI" if c["severity"] == "error" else "CẢNH BÁO"
                column = c["kwargs"].get("column") or c["kwargs"].get("column_A") or ""
                count = f", {c['unexpected_count']} dòng vi phạm" if c["unexpected_count"] else ""
                sample = f", ví dụ {c['sample']}" if c["sample"] else ""
                print(f"    [{label}] {c['expectation']} {column}{count}{sample}")
    status = "ĐẠT" if report["success"] else "KHÔNG ĐẠT"
    print(
        f"Kết quả: {status} ({report['total_checks']} quy tắc, "
        f"{report['failed_errors']} lỗi, {report['failed_warnings']} cảnh báo)"
    )
    report_file = get_settings().quality_dir / f"run={report['run_id']}" / "report.json"
    print(f"Báo cáo: {report_file.resolve()}")
    return report["success"]


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


def cmd_run(keep: int) -> bool:
    entry = run_pipeline(keep)
    return entry["status"] in ("success", "skipped")


def cmd_history(limit: int) -> None:
    runs = read_history(limit)
    if not runs:
        print("Chưa có lần chạy nào")
        return
    print(f"{'Mã lần chạy':<17} {'Trạng thái':<10} {'Thời gian':>9}  {'Đơn':>6}  Ghi chú")
    for r in runs:
        orders = r.get("rows_extracted", {}).get("orders", "")
        # Lỗi có thể dài nhiều dòng: chỉ hiện dòng đầu, chi tiết xem trong data/logs/pipeline.log
        note = (r.get("error") or "").splitlines()[0][:90] if r.get("error") else ""
        if r.get("quality") and not note:
            q = r["quality"]
            note = f"chất lượng: {q['failed_errors']} lỗi, {q['failed_warnings']} cảnh báo"
        print(f"{r['run_id']:<17} {r['status']:<10} {r['duration_seconds']:>8}s  {orders:>6}  {note}")


def main() -> None:
    parser = argparse.ArgumentParser(prog="python -m pipeline", description="ETL pipeline cho THỢ NHANH")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("check", help="Kiểm tra kết nối và đếm số dòng các bảng nguồn")
    sub.add_parser("extract", help="Chụp toàn bộ bảng nguồn ra file Parquet (tầng raw)")
    transform = sub.add_parser("transform", help="Biến đổi dữ liệu raw thành mô hình hình sao")
    transform.add_argument("--run", help="Mã lần extract cần biến đổi (mặc định: lần mới nhất)")
    validate = sub.add_parser("validate", help="Kiểm tra chất lượng kết quả transform (Great Expectations)")
    validate.add_argument("--run", help="Mã lần chạy cần kiểm tra (mặc định: lần mới nhất)")
    load = sub.add_parser("load", help="Nạp kết quả transform vào kho DuckDB")
    load.add_argument("--run", help="Mã lần chạy cần nạp (mặc định: lần mới nhất)")
    load.add_argument("--force", action="store_true", help="Cho phép nạp một lần chạy cũ hơn lần đã nạp")
    run = sub.add_parser("run", help="Chạy cả pipeline: extract, transform, validate, load")
    run.add_argument("--keep", type=int, default=7, help="Số lần chạy gần nhất được giữ lại (mặc định 7)")
    history = sub.add_parser("history", help="Xem lịch sử các lần chạy pipeline")
    history.add_argument("--limit", type=int, default=10, help="Số lần chạy hiển thị (mặc định 10)")
    args = parser.parse_args()

    if args.command == "check":
        cmd_check()
    elif args.command == "extract":
        cmd_extract()
    elif args.command == "transform":
        cmd_transform(args.run)
    elif args.command == "validate":
        if not cmd_validate(args.run):
            sys.exit(1)
    elif args.command == "load":
        cmd_load(args.run, args.force)
    elif args.command == "run":
        if not cmd_run(args.keep):
            sys.exit(1)
    elif args.command == "history":
        cmd_history(args.limit)


if __name__ == "__main__":
    main()