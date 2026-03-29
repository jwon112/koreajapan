-- 일본 CDM ETL 실행 전 검증 쿼리
-- SSMS에서 실행 후 결과를 확인하세요.

-- 1. DB 존재 여부
SELECT name FROM sys.databases
WHERE name IN ('japan_cohort_raw', 'japan_cohort_cdm')
ORDER BY name;

-- 2. japan_cohort_raw 테이블/행 수
USE japan_cohort_raw;

SELECT
    t.name AS table_name,
    SUM(p.rows) AS row_count
FROM sys.tables t
INNER JOIN sys.partitions p ON t.object_id = p.object_id
WHERE p.index_id IN (0, 1)
  AND t.name LIKE 'JP_%'
GROUP BY t.name
ORDER BY t.name;
