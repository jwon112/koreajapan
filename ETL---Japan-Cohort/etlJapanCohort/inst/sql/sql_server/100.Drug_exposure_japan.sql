/**************************************
 Japan: JP_DRUG → Drug_exposure
 who_atc_code/jmdc_drug_code, date_of_prescription, administered_days
**************************************/

INSERT INTO @cdm_database.drug_exposure (
    drug_exposure_id, person_id, drug_concept_id, drug_exposure_start_date, drug_exposure_end_date,
    drug_type_concept_id, quantity, days_supply, visit_occurrence_id, drug_source_value, drug_source_concept_id
)
SELECT
    (SELECT ISNULL(MAX(drug_exposure_id), 0) FROM @cdm_database.drug_exposure)
    + ROW_NUMBER() OVER (ORDER BY r.claim_id, r.statement_id, r.who_atc_code) AS drug_exposure_id,
    p.person_id,
    ISNULL(m.target_concept_id, 0) AS drug_concept_id,
    r.date_of_prescription AS drug_exposure_start_date,
    CASE WHEN r.administered_days > 0
        THEN DATEADD(DAY, r.administered_days - 1, r.date_of_prescription)
        ELSE r.date_of_prescription
    END AS drug_exposure_end_date,
    581452 AS drug_type_concept_id,
    r.administered_amount AS quantity,
    r.administered_days AS days_supply,
    v.visit_occurrence_id,
    COALESCE(r.who_atc_code, CAST(r.jmdc_drug_code AS VARCHAR(50))) AS drug_source_value,
    NULL AS drug_source_concept_id
FROM @raw_database.JP_DRUG r
INNER JOIN @cdm_database.person p ON p.person_source_value = r.member_id
INNER JOIN @cdm_database.visit_occurrence v ON v.visit_source_value = r.claim_id AND v.person_id = p.person_id
LEFT JOIN @cdm_database.source_to_concept_map m ON m.source_code = COALESCE(r.who_atc_code, CAST(r.jmdc_drug_code AS VARCHAR(50))) AND m.domain_id = 'drug' AND m.invalid_reason IS NULL
WHERE NOT EXISTS (
    SELECT 1 FROM @cdm_database.drug_exposure de
    WHERE de.person_id = p.person_id AND de.visit_occurrence_id = v.visit_occurrence_id
    AND de.drug_source_value = COALESCE(r.who_atc_code, CAST(r.jmdc_drug_code AS VARCHAR(50)))
    AND de.drug_exposure_start_date = r.date_of_prescription
);
