# Weather ETL Pipeline (Open-Meteo -> PostgreSQL)

A basic ETL pipeline that retrieves hourly weather data (7-day forecast) for 3 cities from the Open-Meteo API, cleans and transforms the data with Python, and loads it into PostgreSQL using upsert.

## Architecture

Open-Meteo API --(requests)--> transform() --(upsert)--> PostgreSQL (weather_hourly)

- **Extract:** `extract()` calls the API and uses `raise_for_status()` to catch HTTP errors.
- **Transform:** Flattens the JSON response, removes rows with missing values, and adds `forecast_date`, `forecast_hour`, `is_rainy`, and `ingested_at`.
- **Load:** Uses `INSERT ... ON CONFLICT DO UPDATE` with a composite primary key `(city, forecast_time)` to ensure idempotency.

## Database Setup

Create a database named `weather_etl` (`weather_hourly` will be created automatically).

- **pgAdmin:** Right-click **Databases** -> **Create** -> **Database** -> `weather_etl`
- **psql:**
  ```sql
  CREATE DATABASE weather_etl;
  ```

## Configuration

The pipeline reads the database connection settings from environment variables (defaults shown in parentheses):

| Variable | Default |
|----------|---------|
| `PGHOST` | `localhost` |
| `PGPORT` | `5432` |
| `PGDATABASE` | `weather_etl` |
| `PGUSER` | `postgres` |
| `PGPASSWORD` | (empty) |

## Run Locally (Linux/macOS)

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
export PGPASSWORD="your_password"
python etl_pipeline.py
```

## Run Locally (Windows PowerShell)

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
$env:PGPASSWORD = "your_password"
.\.venv\Scripts\python.exe etl_pipeline.py
```

## Run with Docker

The container connects to PostgreSQL running on the host machine:

```bash
docker build -t weather-etl .
docker run --rm \
  --add-host=host.docker.internal:host-gateway \
  -e PGHOST=host.docker.internal \
  -e PGPASSWORD="your_password" \
  weather-etl
```

## Query the Data

- **pgAdmin:** Open **Query Tool** for the `weather_etl` database and paste the contents of `sql/analysis.sql`.
- **psql:**
  ```bash
  psql -U postgres -d weather_etl -f sql/analysis.sql
  ```