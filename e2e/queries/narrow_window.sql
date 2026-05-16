-- 1-week recent window — exercises the gen1-heavy tail. Most recent data
-- still sits in many small gen1 files; compaction should already be
-- promoting older portions of this window to gen2/3.
SELECT region, AVG(temp), COUNT(*)
FROM sensors
WHERE time > now() - INTERVAL '1 week'
GROUP BY region
