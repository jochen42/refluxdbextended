-- 6-hour downsampling over an 8-week window. Touches most parquet files
-- since timestamps span the entire generated range; aggregation work is
-- dominated by group-by and merge across many files. 6h bins keep result
-- cardinality manageable while still exercising the full date range.
SELECT
    DATE_BIN(INTERVAL '6 hours', time) AS bucket,
    region,
    AVG(temp) AS avg_temp,
    APPROX_PERCENTILE_CONT(humidity, 0.95) AS p95_humidity
FROM sensors
WHERE time > now() - INTERVAL '8 weeks'
GROUP BY bucket, region
ORDER BY bucket, region
