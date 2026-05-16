-- High-cardinality predicate: per-host extremes across a 12h window. Bench-
-- marks the per-file pruning path. Compaction should improve this materially
-- when many gen1 files cover overlapping host sets.
SELECT host, MAX(pressure) AS max_p, MIN(pressure) AS min_p
FROM sensors
WHERE time > now() - INTERVAL '12 hours'
GROUP BY host
ORDER BY max_p DESC
LIMIT 50
