-- Narrow recent window — exercises the gen1-heavy tail. With compaction
-- enabled the recent window is still mostly gen1 (compactor hasn't promoted
-- it yet) so this query is the *control* that should be near-identical
-- between the two phases. Useful for sanity-checking the benchmark setup.
SELECT region, AVG(temp), COUNT(*)
FROM sensors
WHERE time > now() - INTERVAL '15 minutes'
GROUP BY region
