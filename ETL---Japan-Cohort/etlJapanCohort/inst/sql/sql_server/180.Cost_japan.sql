/**************************************
 Japan: Cost (minimal)
 - 금액 컬럼이 point 형태로 존재:
     - JP_CLAIMS.total_point → visit_occurrence 비용
     - JP_DRUG.actual_point → drug_exposure 비용
     - JP_PROCEDURE.actual_point → procedure_occurrence 비용
 - 단위(통화) 정보가 없어 currency_concept_id 는 NULL 유지.
 - cost_type_concept_id 는 정보 부족으로 0.
**************************************/

-- 1) Visit 비용 (JP_CLAIMS.total_point)
;WITH vc AS (
    SELECT
        v.visit_occurrence_id AS cost_event_id,
        CAST(N'Visit' AS VARCHAR(20)) AS cost_domain_id,
        CAST(c.total_point AS FLOAT) AS total_cost
    FROM @cdm_database.visit_occurrence v
    JOIN @raw_database.JP_CLAIMS c
      ON c.claim_id = v.visit_source_value
    WHERE c.total_point IS NOT NULL
      AND NOT EXISTS (
          SELECT 1
          FROM @cdm_database.cost x
          WHERE x.cost_domain_id = 'Visit'
            AND x.cost_event_id = v.visit_occurrence_id
      )
)
INSERT INTO @cdm_database.cost (
    cost_id, cost_event_id, cost_domain_id, cost_type_concept_id, currency_concept_id,
    total_charge, total_cost, total_paid, paid_by_payer, paid_by_patient,
    paid_patient_copay, paid_patient_coinsurance, paid_patient_deductible,
    paid_by_primary, paid_ingredient_cost, paid_dispensing_fee, payer_plan_period_id,
    amount_allowed, revenue_code_concept_id, revenue_code_source_value,
    drg_concept_id, drg_source_value
)
SELECT
    (SELECT ISNULL(MAX(cost_id), 0) FROM @cdm_database.cost)
    + ROW_NUMBER() OVER (ORDER BY v.cost_event_id) AS cost_id,
    v.cost_event_id,
    v.cost_domain_id,
    0 AS cost_type_concept_id,
    NULL AS currency_concept_id,
    v.total_cost AS total_charge,
    v.total_cost AS total_cost,
    NULL, NULL, NULL,
    NULL, NULL, NULL,
    NULL, NULL, NULL, NULL,
    NULL, NULL, NULL,
    NULL, NULL
FROM vc v;

-- 2) Drug 비용 (JP_DRUG.actual_point)
;WITH dc AS (
    SELECT
        de.drug_exposure_id AS cost_event_id,
        CAST(N'Drug' AS VARCHAR(20)) AS cost_domain_id,
        CAST(r.actual_point AS FLOAT) AS total_cost
    FROM @raw_database.JP_DRUG r
    JOIN @cdm_database.person p
      ON p.person_source_value = r.member_id
    JOIN @cdm_database.visit_occurrence v
      ON v.visit_source_value = r.claim_id AND v.person_id = p.person_id
    JOIN @cdm_database.drug_exposure de
      ON de.person_id = p.person_id
     AND de.visit_occurrence_id = v.visit_occurrence_id
     AND de.drug_source_value = COALESCE(r.who_atc_code, CAST(r.jmdc_drug_code AS VARCHAR(50)))
     AND de.drug_exposure_start_date = CAST(COALESCE(r.date_of_prescription, r.date_of_dispense, v.visit_start_date, v.visit_end_date) AS DATE)
    WHERE r.actual_point IS NOT NULL
      AND NOT EXISTS (
          SELECT 1
          FROM @cdm_database.cost x
          WHERE x.cost_domain_id = 'Drug'
            AND x.cost_event_id = de.drug_exposure_id
      )
)
INSERT INTO @cdm_database.cost (
    cost_id, cost_event_id, cost_domain_id, cost_type_concept_id, currency_concept_id,
    total_charge, total_cost, total_paid, paid_by_payer, paid_by_patient,
    paid_patient_copay, paid_patient_coinsurance, paid_patient_deductible,
    paid_by_primary, paid_ingredient_cost, paid_dispensing_fee, payer_plan_period_id,
    amount_allowed, revenue_code_concept_id, revenue_code_source_value,
    drg_concept_id, drg_source_value
)
SELECT
    (SELECT ISNULL(MAX(cost_id), 0) FROM @cdm_database.cost)
    + ROW_NUMBER() OVER (ORDER BY d.cost_event_id) AS cost_id,
    d.cost_event_id,
    d.cost_domain_id,
    0 AS cost_type_concept_id,
    NULL AS currency_concept_id,
    d.total_cost AS total_charge,
    d.total_cost AS total_cost,
    NULL, NULL, NULL,
    NULL, NULL, NULL,
    NULL, NULL, NULL, NULL,
    NULL, NULL, NULL,
    NULL, NULL
FROM dc d;

-- 3) Procedure 비용 (JP_PROCEDURE.actual_point)
;WITH pc AS (
    SELECT
        po.procedure_occurrence_id AS cost_event_id,
        CAST(N'Procedure' AS VARCHAR(20)) AS cost_domain_id,
        CAST(pr.actual_point AS FLOAT) AS total_cost
    FROM @raw_database.JP_PROCEDURE pr
    JOIN @cdm_database.person p
      ON p.person_source_value = pr.member_id
    JOIN @cdm_database.visit_occurrence v
      ON v.visit_source_value = pr.claim_id AND v.person_id = p.person_id
    JOIN @cdm_database.procedure_occurrence po
      ON po.person_id = p.person_id
     AND po.visit_occurrence_id = v.visit_occurrence_id
     AND po.procedure_source_value = COALESCE(pr.procedure_code, CAST(pr.standardized_procedure_code AS VARCHAR(50)))
     AND po.procedure_date = CAST(COALESCE(pr.date_of_procedure, v.visit_start_date, v.visit_end_date) AS DATE)
    WHERE pr.actual_point IS NOT NULL
      AND NOT EXISTS (
          SELECT 1
          FROM @cdm_database.cost x
          WHERE x.cost_domain_id = 'Procedure'
            AND x.cost_event_id = po.procedure_occurrence_id
      )
)
INSERT INTO @cdm_database.cost (
    cost_id, cost_event_id, cost_domain_id, cost_type_concept_id, currency_concept_id,
    total_charge, total_cost, total_paid, paid_by_payer, paid_by_patient,
    paid_patient_copay, paid_patient_coinsurance, paid_patient_deductible,
    paid_by_primary, paid_ingredient_cost, paid_dispensing_fee, payer_plan_period_id,
    amount_allowed, revenue_code_concept_id, revenue_code_source_value,
    drg_concept_id, drg_source_value
)
SELECT
    (SELECT ISNULL(MAX(cost_id), 0) FROM @cdm_database.cost)
    + ROW_NUMBER() OVER (ORDER BY p.cost_event_id) AS cost_id,
    p.cost_event_id,
    p.cost_domain_id,
    0 AS cost_type_concept_id,
    NULL AS currency_concept_id,
    p.total_cost AS total_charge,
    p.total_cost AS total_cost,
    NULL, NULL, NULL,
    NULL, NULL, NULL,
    NULL, NULL, NULL, NULL,
    NULL, NULL, NULL,
    NULL, NULL
FROM pc p;

