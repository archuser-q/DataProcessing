import json
import logging
import os
import shutil
import time
from contextlib import contextmanager
from datetime import datetime
from logging.handlers import RotatingFileHandler

import duckdb

from pipeline.config import get_settings
from pipeline.extract import VN_TZ, new_run_id, run_extract
from pipeline.load import run_load
from pipeline.quality import run_quality
from pipeline.transform import run_transform

logger = logging.getLogger("pipeline")
STALE_LOCK_SECONDS = 2 * 60 * 60 


def setup_logging() -> None:
    if logger.handlers:
        return
    logs_dir = get_settings().logs_dir
    logs_dir.mkdir(parents=True, exist_ok=True)
    formatter = logging.Formatter("%(asctime)s %(levelname)-7s %(message)s", "%Y-%m-%d %H:%M:%S")
    file_handler = RotatingFileHandler(
        logs_dir / "pipeline.log", maxBytes=1_000_000, backupCount=5, encoding="utf-8"
    )
    file_handler.setFormatter(formatter)
    console = logging.StreamHandler()
    console.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.addHandler(console)
    logger.setLevel(logging.INFO)


class PipelineLocked(RuntimeError):
    pass


@contextmanager
def pipeline_lock():
    lock = get_settings().data_dir / ".pipeline.lock"
    lock.parent.mkdir(parents=True, exist_ok=True)
    if lock.exists() and time.time() - lock.stat().st_mtime > STALE_LOCK_SECONDS:
        logger.warning("Gỡ khóa cũ sót lại từ lần chạy trước: %s", lock)
        lock.unlink()
    try:
        fd = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        raise PipelineLocked(f"Pipeline đang chạy ở tiến trình khác (khóa {lock})") from None
    try:
        os.write(fd, str(os.getpid()).encode())
        os.close(fd)
        yield
    finally:
        lock.unlink(missing_ok=True)


def _loaded_run_id() -> str | None:
    path = get_settings().warehouse_path
    if not path.exists():
        return None
    con = duckdb.connect(str(path), read_only=True)
    try:
        return con.execute("SELECT MAX(run_id) FROM audit.load_runs").fetchone()[0]
    except duckdb.CatalogException:
        return None
    finally:
        con.close()


def cleanup_old_runs(keep: int) -> list[str]:
    """Xóa dữ liệu của các lần chạy cũ, chỉ giữ `keep` lần gần nhất và lần đang trong kho."""
    settings = get_settings()
    run_ids = sorted(p.stem for p in settings.raw_dir.glob("_runs/*.json"))
    protected = set(run_ids[-keep:]) if keep > 0 else set()
    loaded = _loaded_run_id()
    if loaded:
        protected.add(loaded)

    removed = []
    for run_id in run_ids:
        if run_id in protected:
            continue
        for folder in [
            *settings.raw_dir.glob(f"*/run={run_id}"),
            settings.transformed_dir / f"run={run_id}",
            settings.quality_dir / f"run={run_id}",
        ]:
            shutil.rmtree(folder, ignore_errors=True)
        (settings.raw_dir / "_runs" / f"{run_id}.json").unlink(missing_ok=True)
        removed.append(run_id)
    return removed


def _record(entry: dict) -> None:
    logs_dir = get_settings().logs_dir
    logs_dir.mkdir(parents=True, exist_ok=True)
    with open(logs_dir / "runs.jsonl", "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def run_pipeline(keep: int = 7) -> dict:
    """Chạy cả pipeline. Trả về bản ghi lịch sử; status là success, blocked hoặc failed."""
    setup_logging()
    run_id = new_run_id()
    entry = {
        "run_id": run_id,
        "started_at": datetime.now(VN_TZ).isoformat(timespec="seconds"),
        "status": "running",
        "steps": {},
    }

    def step(name, func, *args):
        logger.info("[%s] bắt đầu %s", run_id, name)
        started = time.perf_counter()
        result = func(*args)
        entry["steps"][name] = round(time.perf_counter() - started, 3)
        logger.info("[%s] xong %s (%.2f giây)", run_id, name, entry["steps"][name])
        return result

    try:
        with pipeline_lock():
            extract = step("extract", run_extract, run_id)
            entry["rows_extracted"] = {t["table"]: t["rows"] for t in extract["tables"]}
            transform = step("transform", run_transform, run_id)
            entry["quarantined_orders"] = transform["outputs"]["quarantine_orders"]

            quality = step("validate", run_quality, run_id)
            entry["quality"] = {
                "success": quality["success"],
                "failed_errors": quality["failed_errors"],
                "failed_warnings": quality["failed_warnings"],
            }
            if not quality["success"]:
                entry["status"] = "blocked"
                logger.error(
                    "[%s] Dữ liệu không đạt %d quy tắc chất lượng, KHÔNG nạp kho",
                    run_id,
                    quality["failed_errors"],
                )
            else:
                load = step("load", run_load, run_id)
                entry["rows_loaded"] = load["tables"]
                entry["status"] = "success"

            removed = cleanup_old_runs(keep)
            if removed:
                logger.info("[%s] Đã dọn %d lần chạy cũ: %s", run_id, len(removed), ", ".join(removed))
    except PipelineLocked as exc:
        entry["status"] = "skipped"
        entry["error"] = str(exc)
        logger.warning("[%s] %s", run_id, exc)
    except Exception as exc:  # ghi lại mọi lỗi để xem trong lịch sử
        entry["status"] = "failed"
        entry["error"] = f"{type(exc).__name__}: {exc}"
        logger.exception("[%s] Pipeline lỗi", run_id)

    entry["finished_at"] = datetime.now(VN_TZ).isoformat(timespec="seconds")
    entry["duration_seconds"] = round(sum(entry["steps"].values()), 3)
    _record(entry)
    logger.info("[%s] Kết thúc: %s", run_id, entry["status"])
    return entry


def read_history(limit: int = 10) -> list[dict]:
    path = get_settings().logs_dir / "runs.jsonl"
    if not path.exists():
        return []
    lines = path.read_text(encoding="utf-8").splitlines()
    return [json.loads(line) for line in lines[-limit:]]