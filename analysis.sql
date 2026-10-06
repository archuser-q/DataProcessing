SELECT
    city,
    forecast_date,
    ROUND(AVG(temperature_c), 1) AS avg_temp_c,
    MAX(temperature_c)           AS max_temp_c,
    MIN(temperature_c)           AS min_temp_c
FROM weather_hourly
GROUP BY city, forecast_date
ORDER BY city, forecast_date;

SELECT
    city,
    SUM(is_rainy)                    AS rainy_hours,
    ROUND(SUM(precipitation_mm), 1)  AS total_rain_mm
FROM weather_hourly
GROUP BY city
ORDER BY total_rain_mm DESC;

WITH daily AS (
    SELECT
        city,
        forecast_date,
        MAX(temperature_c) AS max_temp_c
    FROM weather_hourly
    GROUP BY city, forecast_date
),
ranked AS (
    SELECT
        city,
        forecast_date,
        max_temp_c,
        RANK() OVER (PARTITION BY city ORDER BY max_temp_c DESC) AS temp_rank
    FROM daily
)
SELECT city, forecast_date, max_temp_c
FROM ranked
WHERE temp_rank = 1;

SELECT
    city,
    COUNT(*)                                          AS total_rows,
    COUNT(DISTINCT forecast_time)                     AS distinct_hours,
    SUM(CASE WHEN temperature_c IS NULL THEN 1 END)   AS null_temp_rows
FROM weather_hourly
GROUP BY city;