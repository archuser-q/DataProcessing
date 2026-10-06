# Weather ETL Pipeline (Open-Meteo -> PostgreSQL)

ETL pipeline co ban: lay du lieu thoi tiet theo gio (7 ngay) cua 3 thanh pho tu
Open-Meteo API, lam sach/bien doi bang Python, nap vao PostgreSQL bang upsert.

## Kien truc

    Open-Meteo API --(requests)--> transform() --(upsert)--> PostgreSQL (weather_hourly)

- Extract: `extract()` goi API, `raise_for_status()` de bat loi HTTP
- Transform: flatten JSON, bo dong thieu gia tri, them `forecast_date`, `forecast_hour`, `is_rainy`, `ingested_at`
- Load: `INSERT ... ON CONFLICT DO UPDATE` voi khoa chinh (city, forecast_time) -> idempotent

## Chuan bi database

Tao database `weather_etl` (bang `weather_hourly` se duoc tao tu dong).

- pgAdmin: chuot phai Databases -> Create -> Database -> `weather_etl`
- hoac psql: `CREATE DATABASE weather_etl;`

## Cau hinh

Pipeline doc ket noi tu bien moi truong (mac dinh trong ngoac):

| Bien         | Mac dinh      |
|--------------|---------------|
| `PGHOST`     | `localhost`   |
| `PGPORT`     | `5432`        |
| `PGDATABASE` | `weather_etl` |
| `PGUSER`     | `postgres`    |
| `PGPASSWORD` | (rong)        |

## Chay local (Linux/macOS)

    python3 -m venv .venv
    source .venv/bin/activate
    pip install -r requirements.txt
    export PGPASSWORD="mat_khau_cua_ban"
    python etl_pipeline.py

## Chay local (Windows PowerShell)

    python -m venv .venv
    .\.venv\Scripts\python.exe -m pip install -r requirements.txt
    $env:PGPASSWORD = "mat_khau_cua_ban"
    .\.venv\Scripts\python.exe etl_pipeline.py

## Chay bang Docker

Container ket noi toi PostgreSQL dang chay tren may host:

    docker build -t weather-etl .
    docker run --rm \
      --add-host=host.docker.internal:host-gateway \
      -e PGHOST=host.docker.internal \
      -e PGPASSWORD="mat_khau_cua_ban" \
      weather-etl

## Query du lieu

- pgAdmin: mo Query Tool tren database `weather_etl`, paste noi dung `sql/analysis.sql`
- hoac psql: `psql -U postgres -d weather_etl -f sql/analysis.sql`

## Len lich (cron, Linux)

    0 * * * * cd /path/to/weather-etl && PGPASSWORD="mat_khau_cua_ban" .venv/bin/python etl_pipeline.py >> etl.log 2>&1