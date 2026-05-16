-- Hourly downsampling over a long window. Touches most parquet files since
-- timestamps span the entire generated range; aggregation work is dominated
-- by group-by and merge across many files.
SELECT
    DATE_BIN('1 hour', time) AS bucket,
    region,
    AVG(temp) AS avg_temp,
    APPROX_PERCENTILE_CONT(humidity, 0.95) AS p95_humidity
FROM sensors
WHERE time > now() - INTERVAL '24 hours'
GROUP BY bucket, region
ORDER BY bucket, region
