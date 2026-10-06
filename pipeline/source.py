from functools import lru_cache

from sqlalchemy import Engine, create_engine, text

from pipeline.config import get_settings


@lru_cache
def get_engine() -> Engine:
    return create_engine(get_settings().source_database_url, pool_pre_ping=True)


def table_row_counts(tables: list[str]) -> dict[str, int]:
    with get_engine().connect() as conn:
        return {t: conn.execute(text(f'SELECT COUNT(*) FROM "{t}"')).scalar_one() for t in tables}