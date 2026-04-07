/**************************************
 Japan: JP_DIAGNOSIS → Condition_occurrence
 icd10_level4_code, date_of_medical_care_start, member_id, claim_id
 condition_start_date: NOT NULL — 원본 일자 NULL 이면 visit_start_date / visit_end_date 로 보완

 성능: OUTER APPLY(source_to_concept_map) + NOT EXISTS(condition_occurrence) 는 인덱스 없으면
 수백만 행에서 수 일 단위로 늘어날 수 있음(무한루프 아님). 아래 인덱스는 1회 생성 후 재사용.
**************************************/

-- ETL 성능용 인덱스(이미 있으면 스킵)
IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes i
    INNER JOIN sys.tables t ON i.object_id = t.object_id
    WHERE i.name = N'IX_stcm_source_code_japan_etl'
      AND t.name = N'source_to_concept_map'
      AND SCHEMA_NAME(t.schema_id) = N'dbo'
)
    CREATE NONCLUSTERED INDEX IX_stcm_source_code_japan_etl
        ON dbo.source_to_concept_map (source_code)
        INCLUDE (target_concept_id, invalid_reason, domain_id);
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes i
    INNER JOIN sys.tables t ON i.object_id = t.object_id
    WHERE i.name = N'IX_co_etl_dedup_japan'
      AND t.name = N'condition_occurrence'
      AND SCHEMA_NAME(t.schema_id) = N'dbo'
)
    CREATE NONCLUSTERED INDEX IX_co_etl_dedup_japan
        ON dbo.condition_occurrence (person_id, visit_occurrence_id, condition_source_value, condition_start_date);
GO

INSERT INTO @cdm_database.condition_occurrence (
    condition_occurrence_id, person_id, condition_concept_id, condition_start_date, condition_end_date,
    condition_type_concept_id, visit_occurrence_id, condition_source_value, condition_source_concept_id, master_seq
)
SELECT
    (SELECT ISNULL(MAX(condition_occurrence_id), 0) FROM @cdm_database.condition_occurrence)
    + ROW_NUMBER() OVER (ORDER BY d.claim_id, d.icd10_level4_code, d.statement_id) AS condition_occurrence_id,
    p.person_id,
    -- [Mapping policy]
    --  - FALLBACK 허용: Condition 없으면 Observation → Measurement → Spec Anatomic Site (ICD가 Measurement에만 있는 경우 많음, 예 R73.0)
    --  - FALLBACK 금지(참고): Condition domain_id로만 매칭하고 없으면 0 유지(Unknown)
    ISNULL(cm_map.target_concept_id, 0) AS condition_concept_id,
    dt.cond_date AS condition_start_date,
    dt.cond_date AS condition_end_date,
    CASE WHEN d.main_disease_flag = 1 THEN 44786627 ELSE 44786629 END AS condition_type_concept_id,
    v.visit_occurrence_id,
    d.icd10_level4_code AS condition_source_value,
    NULL AS condition_source_concept_id,
    sm.master_seq
FROM @raw_database.JP_DIAGNOSIS d
INNER JOIN @cdm_database.person p ON p.person_source_value = d.member_id
INNER JOIN @cdm_database.visit_occurrence v ON v.visit_source_value = d.claim_id AND v.person_id = p.person_id
-- ICD10 코드 포맷 정규화(매칭 input을 맞추기 위함)
CROSS APPLY (
    SELECT
        CASE
            WHEN REPLACE(
                    REPLACE(UPPER(LTRIM(RTRIM(d.icd10_level4_code))), '-', ''),
                '.', ''
            ) LIKE '[A-Z][0-9][0-9][0-9]'
            THEN LEFT(
                    REPLACE(
                        REPLACE(UPPER(LTRIM(RTRIM(d.icd10_level4_code))), '-', ''),
                    '.', ''),
                    3
                )
                + '.'
                + RIGHT(
                    REPLACE(
                        REPLACE(UPPER(LTRIM(RTRIM(d.icd10_level4_code))), '-', ''),
                    '.', ''),
                    1
                )
            ELSE REPLACE(UPPER(LTRIM(RTRIM(d.icd10_level4_code))), '-', '')
        END AS norm_code
) nc
/* [Mapping policy - FALLBACK 허용]
   Condition 매핑이 있으면 그걸 우선 사용.
   없으면 Observation → Measurement → Spec Anatomic Site 순으로 fallback.
   (Vocabulary 상 ICD 코드가 Measurement 도메인 concept만 가질 때가 있음 — condition_occurrence에는 비표준이나 연구 목적상 허용)
*/
OUTER APPLY (
    SELECT TOP (1)
        cm.target_concept_id
    FROM @cdm_database.source_to_concept_map cm
    WHERE cm.source_code = nc.norm_code
      AND cm.invalid_reason IS NULL
      AND LOWER(cm.domain_id) IN ('condition', 'observation', 'measurement', 'spec anatomic site')
    ORDER BY
        CASE
            WHEN LOWER(cm.domain_id) = 'condition' THEN 1
            WHEN LOWER(cm.domain_id) = 'observation' THEN 2
            WHEN LOWER(cm.domain_id) = 'measurement' THEN 3
            WHEN LOWER(cm.domain_id) = 'spec anatomic site' THEN 4
            ELSE 5
        END,
        cm.target_concept_id
) cm_map
/*
-- [Mapping policy - FALLBACK 금지(참고)]
-- Condition domain에 매핑되는 경우만 사용. 없으면 Unknown(0) 유지.
OUTER APPLY (
    SELECT TOP (1)
        cm.target_concept_id
    FROM @cdm_database.source_to_concept_map cm
    WHERE cm.source_code = nc.norm_code
      AND cm.invalid_reason IS NULL
      AND LOWER(cm.domain_id) = 'condition'
) cm_map
*/
OUTER APPLY (
    SELECT TOP (1) x.master_seq
    FROM @cdm_database.SEQ_MASTER x
    WHERE x.source_table = 'DIA'
      AND x.member_id = CAST(d.member_id AS VARCHAR(50))
      AND x.claim_id = CAST(d.claim_id AS VARCHAR(50))
      AND (
          (x.statement_id = CAST(d.statement_id AS FLOAT) AND d.statement_id IS NOT NULL)
          OR (x.statement_id IS NULL AND d.statement_id IS NULL)
      )
) sm
CROSS APPLY (
    SELECT CAST(COALESCE(
        d.date_of_medical_care_start,
        v.visit_start_date,
        v.visit_end_date
    ) AS DATE) AS cond_date
) dt
WHERE d.icd10_level4_code IS NOT NULL AND RTRIM(d.icd10_level4_code) <> ''
  AND dt.cond_date IS NOT NULL
AND NOT EXISTS (
    SELECT 1 FROM @cdm_database.condition_occurrence co
    WHERE co.person_id = p.person_id AND co.condition_source_value = d.icd10_level4_code
    AND co.visit_occurrence_id = v.visit_occurrence_id AND co.condition_start_date = dt.cond_date
);
