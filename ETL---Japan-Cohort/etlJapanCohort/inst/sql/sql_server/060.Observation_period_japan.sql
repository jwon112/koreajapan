/**************************************
 Japan: JP_PATIENT.observation_start/end 또는 JP_CLAIMS 기간
 observation_start, observation_end (varchar YYYYMMDD?) 사용
**************************************/

INSERT INTO @cdm_database.observation_period (
    person_id, observation_period_start_date, observation_period_end_date, period_type_concept_id
)
SELECT
    p.person_id,
    CASE
        WHEN LEN(RTRIM(pt.observation_start)) >= 8
        THEN TRY_CAST(SUBSTRING(RTRIM(pt.observation_start), 1, 4) + '-' + SUBSTRING(RTRIM(pt.observation_start), 5, 2) + '-' + SUBSTRING(RTRIM(pt.observation_start), 7, 2) AS DATE)
        WHEN LEN(RTRIM(pt.observation_start)) = 6
        THEN TRY_CAST(SUBSTRING(RTRIM(pt.observation_start), 1, 4) + '-' + SUBSTRING(RTRIM(pt.observation_start), 5, 2) + '-01' AS DATE)
        ELSE NULL
    END AS observation_period_start_date,
    CASE
        WHEN LEN(RTRIM(pt.observation_end)) >= 8
        THEN TRY_CAST(SUBSTRING(RTRIM(pt.observation_end), 1, 4) + '-' + SUBSTRING(RTRIM(pt.observation_end), 5, 2) + '-' + SUBSTRING(RTRIM(pt.observation_end), 7, 2) AS DATE)
        WHEN LEN(RTRIM(pt.observation_end)) = 6
        THEN EOMONTH(TRY_CAST(SUBSTRING(RTRIM(pt.observation_end), 1, 4) + '-' + SUBSTRING(RTRIM(pt.observation_end), 5, 2) + '-01' AS DATE))
        ELSE NULL
    END AS observation_period_end_date,
    44814725 AS period_type_concept_id
FROM @raw_database.JP_PATIENT pt
INNER JOIN @cdm_database.person p ON p.person_source_value = pt.member_id
WHERE LEN(RTRIM(pt.observation_start)) >= 6 AND LEN(RTRIM(pt.observation_end)) >= 6
AND p.person_id NOT IN (SELECT person_id FROM @cdm_database.observation_period);
