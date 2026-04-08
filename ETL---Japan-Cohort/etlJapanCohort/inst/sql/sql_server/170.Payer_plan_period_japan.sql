/**************************************
 Japan: Payer_plan_period (minimal)
 - JP_PATIENT.observation_start/end 를 보험/관측 기간으로 사용(문자열 → DATE 변환 가능한 행만).
 - payer/plan/sponsor 정보는 원천 컬럼이 없어 NULL 유지.
**************************************/

;WITH pt AS (
    SELECT
        p.person_id,
        TRY_CONVERT(DATE, pt.observation_start, 112) AS start_ymd,
        TRY_CONVERT(DATE, pt.observation_end, 112) AS end_ymd,
        TRY_CONVERT(DATE, pt.observation_start) AS start_any,
        TRY_CONVERT(DATE, pt.observation_end) AS end_any
    FROM @raw_database.JP_PATIENT pt
    JOIN @cdm_database.person p
      ON p.person_source_value = pt.member_id
),
norm AS (
    SELECT
        person_id,
        COALESCE(start_ymd, start_any) AS start_date,
        COALESCE(end_ymd, end_any) AS end_date
    FROM pt
),
ready AS (
    SELECT
        n.person_id,
        n.start_date,
        n.end_date
    FROM norm n
    WHERE n.start_date IS NOT NULL
      AND n.end_date IS NOT NULL
      AND n.end_date >= n.start_date
      AND NOT EXISTS (
          SELECT 1
          FROM @cdm_database.payer_plan_period ppp
          WHERE ppp.person_id = n.person_id
      )
)
INSERT INTO @cdm_database.payer_plan_period (
    payer_plan_period_id, person_id, payer_plan_period_start_date, payer_plan_period_end_date,
    payer_concept_id, payer_source_value, payer_source_concept_id,
    plan_concept_id, plan_source_value, plan_source_concept_id,
    sponsor_concept_id, sponsor_source_value, sponsor_source_concept_id,
    family_source_value, stop_reason_concept_id, stop_reason_source_value, stop_reason_source_concept_id
)
SELECT
    (SELECT ISNULL(MAX(payer_plan_period_id), 0) FROM @cdm_database.payer_plan_period)
    + ROW_NUMBER() OVER (ORDER BY r.person_id) AS payer_plan_period_id,
    r.person_id,
    r.start_date,
    r.end_date,
    NULL, NULL, NULL,
    NULL, NULL, NULL,
    NULL, NULL, NULL,
    NULL, NULL, NULL, NULL
FROM ready r;

