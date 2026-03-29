/**************************************
 Japan: JP_PATIENT.withdrawal_death_flag → Death
 관찰 종료 시점에 사망 플래그가 있으면 death_date = observation_end
**************************************/

INSERT INTO @cdm_database.death (
    person_id, death_date, death_datetime, death_type_concept_id, cause_concept_id, cause_source_value, cause_source_concept_id
)
SELECT
    p.person_id,
    CASE
        WHEN LEN(RTRIM(pt.observation_end)) >= 8
        THEN TRY_CAST(SUBSTRING(RTRIM(pt.observation_end), 1, 4) + '-' + SUBSTRING(RTRIM(pt.observation_end), 5, 2) + '-' + SUBSTRING(RTRIM(pt.observation_end), 7, 2) AS DATE)
        WHEN LEN(RTRIM(pt.observation_end)) = 6
        THEN EOMONTH(TRY_CAST(SUBSTRING(RTRIM(pt.observation_end), 1, 4) + '-' + SUBSTRING(RTRIM(pt.observation_end), 5, 2) + '-01' AS DATE))
        ELSE NULL
    END AS death_date,
    NULL AS death_datetime,
    38003566 AS death_type_concept_id,
    NULL AS cause_concept_id,
    pt.withdrawal_death_flag AS cause_source_value,
    NULL AS cause_source_concept_id
FROM @raw_database.JP_PATIENT pt
INNER JOIN @cdm_database.person p ON p.person_source_value = pt.member_id
WHERE pt.withdrawal_death_flag IS NOT NULL AND RTRIM(pt.withdrawal_death_flag) <> ''
AND LEN(RTRIM(pt.observation_end)) >= 6
AND p.person_id NOT IN (SELECT person_id FROM @cdm_database.death);
