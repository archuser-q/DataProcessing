import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


@dataclass(frozen=True)
class Settings:
    source_database_url: str
    data_dir: Path

    @property
    def raw_dir(self) -> Path:
        return self.data_dir / "raw"

    @property
    def transformed_dir(self) -> Path:
        return self.data_dir / "transformed"

    @property
    def quality_dir(self) -> Path:
        return self.data_dir / "quality"

    @property
    def logs_dir(self) -> Path:
        return self.data_dir / "logs"

    @property
    def warehouse_path(self) -> Path:
        return self.data_dir / "warehouse.duckdb"


def get_settings() -> Settings:
    url = os.getenv("SOURCE_DATABASE_URL")
    if not url:
        raise RuntimeError("Chưa đặt SOURCE_DATABASE_URL trong file .env")
    return Settings(source_database_url=url, data_dir=Path(os.getenv("DATA_DIR", "data")))