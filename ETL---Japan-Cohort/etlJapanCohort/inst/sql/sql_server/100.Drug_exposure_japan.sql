/**************************************
 Japan: JP_DRUG → Drug_exposure
 who_atc_code/jmdc_drug_code, date_of_prescription, administered_days
 drug_exposure_start_date NOT NULL — 처방일 NULL 이면 visit_start_date / visit_end_date 로 보완

 [Mapping policy]
  - 원천 키: COALESCE(who_atc_code, jmdc_drug_code) → TRIM + UPPER(norm_drug_code)
  - 1) source_to_concept_map: domain Drug 또는 Ingredient (OMOP에서 둘 다 domain_id=Drug 인 경우도 많음)
  - 2) 같은 source_code 로 target이 RxNorm 인 행 (domain 표기가 Drug가 아닌 예외 대비)
  - 3) ATC 상위 코드: 짧은 접두(7→5→4→3자)로 stm 재조회
  - 4) 최종 폴백: CONCEPT 에 ATC 행은 있는데 CONCEPT_RELATIONSHIP 에 Maps to 가 없으면 stm 이 비어 있음(stm_rows=0).
     이 경우 drug_concept_id 에 ATC 분류 concept_id 를 넣음(엄밀한 RxNorm 표준 아님 — 분석 시 주의, 0 보다 나은 절충).

 성능: NOT EXISTS용 IX_de_etl_dedup_japan, map 은 IX_stcm_source_code_japan_etl, CONCEPT 조회용 IX_concept_voc_code_japan_etl
        (필터 인덱스 WHERE vocabulary_id=… 는 열/상수 형식 불일치로 오류 10611 날 수 있어 비필터 사용)
**************************************/

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes i
    INNER JOIN sys.tables t ON i.object_id = t.object_id
    WHERE i.name = N'IX_concept_voc_code_japan_etl'
      AND t.name = N'concept'
      AND SCHEMA_NAME(t.schema_id) = N'dbo'
)
    CREATE NONCLUSTERED INDEX IX_concept_voc_code_japan_etl
        ON dbo.concept (vocabulary_id, concept_code)
        INCLUDE (concept_id);
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes i
    INNER JOIN sys.tables t ON i.object_id = t.object_id
    WHERE i.name = N'IX_de_etl_dedup_japan'
      AND t.name = N'drug_exposure'
      AND SCHEMA_NAME(t.schema_id) = N'dbo'
)
    CREATE NONCLUSTERED INDEX IX_de_etl_dedup_japan
        ON dbo.drug_exposure (person_id, visit_occurrence_id, drug_source_value, drug_exposure_start_date);
GO

INSERT INTO @cdm_database.drug_exposure (
    drug_exposure_id, person_id, drug_concept_id, drug_exposure_start_date, drug_exposure_end_date,
    drug_type_concept_id, quantity, days_supply, visit_occurrence_id, drug_source_value, drug_source_concept_id, master_seq
)
SELECT
    (SELECT ISNULL(MAX(drug_exposure_id), 0) FROM @cdm_database.drug_exposure)
    + ROW_NUMBER() OVER (ORDER BY r.claim_id, r.statement_id, r.who_atc_code) AS drug_exposure_id,
    p.person_id,
    COALESCE(
        cm_exact.target_concept_id,
        cm_atc5.target_concept_id,
        cm_atc4.target_concept_id,
        cm_atc3.target_concept_id,
        atc_cls.concept_id,
        0
    ) AS drug_concept_id,
    dt.drug_dt AS drug_exposure_start_date,
    CASE WHEN ISNULL(r.administered_days, 0) > 0
        THEN DATEADD(DAY, r.administered_days - 1, dt.drug_dt)
        ELSE dt.drug_dt
    END AS drug_exposure_end_date,
    581452 AS drug_type_concept_id,
    r.administered_amount AS quantity,
    r.administered_days AS days_supply,
    v.visit_occurrence_id,
    COALESCE(r.who_atc_code, CAST(r.jmdc_drug_code AS VARCHAR(50))) AS drug_source_value,
    NULL AS drug_source_concept_id,
    sm.master_seq
FROM @raw_database.JP_DRUG r
INNER JOIN @cdm_database.person p ON p.person_source_value = r.member_id
INNER JOIN @cdm_database.visit_occurrence v ON v.visit_source_value = r.claim_id AND v.person_id = p.person_id
CROSS APPLY (
    SELECT UPPER(LTRIM(RTRIM(COALESCE(r.who_atc_code, CAST(r.jmdc_drug_code AS VARCHAR(50)))))) AS norm_drug_code
) nd
-- 정확 일치: Drug/Ingredient domain 이거나 RxNorm 타깃(도메인 표기 예외)
OUTER APPLY (
    SELECT TOP (1)
        m.target_concept_id
    FROM @cdm_database.source_to_concept_map m
    WHERE m.source_code = nd.norm_drug_code
      AND m.invalid_reason IS NULL
      AND (
          LOWER(m.domain_id) IN (N'drug', N'ingredient')
          OR m.target_vocabulary_id LIKE N'RxNorm%'
      )
    ORDER BY
        CASE
            WHEN LOWER(m.domain_id) IN (N'drug', N'ingredient') THEN 1
            ELSE 2
        END,
        CASE WHEN m.target_vocabulary_id LIKE N'RxNorm%' THEN 1 ELSE 2 END,
        m.target_concept_id
) cm_exact
-- ATC 상위 레벨(접두) — stm 에 하위 코드만 없고 상위만 있는 경우
OUTER APPLY (
    SELECT TOP (1)
        m.target_concept_id
    FROM @cdm_database.source_to_concept_map m
    WHERE LEN(nd.norm_drug_code) > 5
      AND m.source_code = LEFT(nd.norm_drug_code, 5)
      AND m.invalid_reason IS NULL
      AND (
          LOWER(m.domain_id) IN (N'drug', N'ingredient')
          OR m.target_vocabulary_id LIKE N'RxNorm%'
      )
    ORDER BY
        CASE WHEN LOWER(m.domain_id) IN (N'drug', N'ingredient') THEN 1 ELSE 2 END,
        m.target_concept_id
) cm_atc5
OUTER APPLY (
    SELECT TOP (1)
        m.target_concept_id
    FROM @cdm_database.source_to_concept_map m
    WHERE LEN(nd.norm_drug_code) > 4
      AND m.source_code = LEFT(nd.norm_drug_code, 4)
      AND m.invalid_reason IS NULL
      AND (
          LOWER(m.domain_id) IN (N'drug', N'ingredient')
          OR m.target_vocabulary_id LIKE N'RxNorm%'
      )
    ORDER BY
        CASE WHEN LOWER(m.domain_id) IN (N'drug', N'ingredient') THEN 1 ELSE 2 END,
        m.target_concept_id
) cm_atc4
OUTER APPLY (
    SELECT TOP (1)
        m.target_concept_id
    FROM @cdm_database.source_to_concept_map m
    WHERE LEN(nd.norm_drug_code) > 3
      AND m.source_code = LEFT(nd.norm_drug_code, 3)
      AND m.invalid_reason IS NULL
      AND (
          LOWER(m.domain_id) IN (N'drug', N'ingredient')
          OR m.target_vocabulary_id LIKE N'RxNorm%'
      )
    ORDER BY
        CASE WHEN LOWER(m.domain_id) IN (N'drug', N'ingredient') THEN 1 ELSE 2 END,
        m.target_concept_id
) cm_atc3
-- Maps to 가 없어 stm 에 행이 없을 때: ATC 분류 concept 자체(표준 RxNorm 아님)
OUTER APPLY (
    SELECT TOP (1)
        c1.concept_id
    FROM @cdm_database.CONCEPT c1
    WHERE c1.vocabulary_id = N'ATC'
      AND c1.concept_code = nd.norm_drug_code
      AND (c1.invalid_reason IS NULL OR c1.invalid_reason = N'')
    ORDER BY c1.concept_id
) atc_cls
OUTER APPLY (
    SELECT TOP (1) x.master_seq
    FROM @cdm_database.SEQ_MASTER x
    WHERE x.source_table = 'DRG'
      AND x.member_id = CAST(r.member_id AS VARCHAR(50))
      AND x.claim_id = CAST(r.claim_id AS VARCHAR(50))
      AND (
          (x.statement_id = CAST(r.statement_id AS FLOAT) AND r.statement_id IS NOT NULL)
          OR (x.statement_id IS NULL AND r.statement_id IS NULL)
      )
) sm
CROSS APPLY (
    SELECT CAST(COALESCE(
        r.date_of_prescription,
        v.visit_start_date,
        v.visit_end_date
    ) AS DATE) AS drug_dt
) dt
WHERE dt.drug_dt IS NOT NULL
AND NOT EXISTS (
    SELECT 1 FROM @cdm_database.drug_exposure de
    WHERE de.person_id = p.person_id AND de.visit_occurrence_id = v.visit_occurrence_id
    AND de.drug_source_value = COALESCE(r.who_atc_code, CAST(r.jmdc_drug_code AS VARCHAR(50)))
    AND de.drug_exposure_start_date = dt.drug_dt
);
