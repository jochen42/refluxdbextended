-- Full-range aggregation across every host and every point. Forces a scan of
-- every parquet file the querier knows about. Most-uncompacted-friendly:
-- many small gen1 files = many small reads.
SELECT region, AVG(temp) AS avg_temp, MAX(humidity) AS max_humid, COUNT(*) AS n
FROM sensors
GROUP BY region
