/**************************************
 Japan: JP_CLAIMS → Visit_occurrence
 claim_id(varchar), member_id, admission_date/discharge_date, type_of_claim
 visit_occurrence_id: bigint 필요 → ROW_NUMBER로 생성
**************************************/

INSERT INTO @cdm_database.visit_occurrence (
    visit_occurrence_id, person_id, visit_concept_id, visit_start_date, visit_start_datetime,
    visit_end_date, visit_end_datetime, visit_type_concept_id, provider_id, care_site_id,
    visit_source_value, visit_source_concept_id
)
SELECT
    (SELECT ISNULL(MAX(visit_occurrence_id), 0) FROM @cdm_database.visit_occurrence)
    + ROW_NUMBER() OVER (ORDER BY c.claim_id) AS visit_occurrence_id,
    p.person_id,
    CASE
        WHEN c.type_of_claim IN ('inpatient', 'Inpatient', '1', '입원') THEN 9201
        WHEN c.type_of_claim IN ('outpatient', 'Outpatient', '2', '외래') THEN 9202
        ELSE 9202
    END AS visit_concept_id,
    COALESCE(c.admission_date, c.discharge_date,
        TRY_CONVERT(DATE, CAST(CAST(c.month_and_year_of_medical_care AS INT) AS VARCHAR) + '01', 112)) AS visit_start_date,
    NULL AS visit_start_datetime,
    COALESCE(c.discharge_date, c.admission_date,
        EOMONTH(TRY_CONVERT(DATE, CAST(CAST(c.month_and_year_of_medical_care AS INT) AS VARCHAR) + '01', 112))) AS visit_end_date,
    NULL AS visit_end_datetime,
    44818517 AS visit_type_concept_id,
    NULL AS provider_id,
    NULL AS care_site_id,
    c.claim_id AS visit_source_value,
    NULL AS visit_source_concept_id
FROM @raw_database.JP_CLAIMS c
INNER JOIN @cdm_database.person p ON p.person_source_value = c.member_id
WHERE c.claim_id NOT IN (SELECT visit_source_value FROM @cdm_database.visit_occurrence);
