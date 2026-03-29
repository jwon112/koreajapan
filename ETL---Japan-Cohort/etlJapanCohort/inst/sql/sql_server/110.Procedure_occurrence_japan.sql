/**************************************
 Japan: JP_PROCEDURE → Procedure_occurrence
 procedure_code, date_of_procedure, standardized_procedure_code
**************************************/

INSERT INTO @cdm_database.procedure_occurrence (
    procedure_occurrence_id, person_id, procedure_concept_id, procedure_date, procedure_datetime,
    procedure_type_concept_id, visit_occurrence_id, procedure_source_value, procedure_source_concept_id
)
SELECT
    (SELECT ISNULL(MAX(procedure_occurrence_id), 0) FROM @cdm_database.procedure_occurrence)
    + ROW_NUMBER() OVER (ORDER BY pr.claim_id, pr.statement_id, pr.procedure_code) AS procedure_occurrence_id,
    p.person_id,
    ISNULL(m.target_concept_id, 0) AS procedure_concept_id,
    pr.date_of_procedure AS procedure_date,
    NULL AS procedure_datetime,
    44818517 AS procedure_type_concept_id,
    v.visit_occurrence_id,
    COALESCE(pr.procedure_code, CAST(pr.standardized_procedure_code AS VARCHAR(50))) AS procedure_source_value,
    NULL AS procedure_source_concept_id
FROM @raw_database.JP_PROCEDURE pr
INNER JOIN @cdm_database.person p ON p.person_source_value = pr.member_id
INNER JOIN @cdm_database.visit_occurrence v ON v.visit_source_value = pr.claim_id AND v.person_id = p.person_id
LEFT JOIN @cdm_database.source_to_concept_map m ON m.source_code = COALESCE(pr.procedure_code, CAST(pr.standardized_procedure_code AS VARCHAR(50))) AND m.domain_id = 'procedure' AND m.invalid_reason IS NULL
WHERE NOT EXISTS (
    SELECT 1 FROM @cdm_database.procedure_occurrence po
    WHERE po.person_id = p.person_id AND po.visit_occurrence_id = v.visit_occurrence_id
    AND po.procedure_source_value = COALESCE(pr.procedure_code, CAST(pr.standardized_procedure_code AS VARCHAR(50)))
    AND po.procedure_date = pr.date_of_procedure
);
