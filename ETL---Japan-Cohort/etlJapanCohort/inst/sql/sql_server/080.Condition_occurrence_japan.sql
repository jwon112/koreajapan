/**************************************
 Japan: JP_DIAGNOSIS → Condition_occurrence
 icd10_level4_code, date_of_medical_care_start, member_id, claim_id
**************************************/

INSERT INTO @cdm_database.condition_occurrence (
    condition_occurrence_id, person_id, condition_concept_id, condition_start_date, condition_end_date,
    condition_type_concept_id, visit_occurrence_id, condition_source_value, condition_source_concept_id
)
SELECT
    (SELECT ISNULL(MAX(condition_occurrence_id), 0) FROM @cdm_database.condition_occurrence)
    + ROW_NUMBER() OVER (ORDER BY d.claim_id, d.icd10_level4_code, d.statement_id) AS condition_occurrence_id,
    p.person_id,
    ISNULL(m.target_concept_id, 0) AS condition_concept_id,
    d.date_of_medical_care_start AS condition_start_date,
    d.date_of_medical_care_start AS condition_end_date,
    CASE WHEN d.main_disease_flag = 1 THEN 44786627 ELSE 44786629 END AS condition_type_concept_id,
    v.visit_occurrence_id,
    d.icd10_level4_code AS condition_source_value,
    NULL AS condition_source_concept_id
FROM @raw_database.JP_DIAGNOSIS d
INNER JOIN @cdm_database.person p ON p.person_source_value = d.member_id
INNER JOIN @cdm_database.visit_occurrence v ON v.visit_source_value = d.claim_id AND v.person_id = p.person_id
LEFT JOIN @cdm_database.source_to_concept_map m ON m.source_code = d.icd10_level4_code AND m.domain_id = 'condition' AND m.invalid_reason IS NULL
WHERE d.icd10_level4_code IS NOT NULL AND RTRIM(d.icd10_level4_code) <> ''
AND NOT EXISTS (
    SELECT 1 FROM @cdm_database.condition_occurrence co
    WHERE co.person_id = p.person_id AND co.condition_source_value = d.icd10_level4_code
    AND co.visit_occurrence_id = v.visit_occurrence_id AND co.condition_start_date = d.date_of_medical_care_start
);
