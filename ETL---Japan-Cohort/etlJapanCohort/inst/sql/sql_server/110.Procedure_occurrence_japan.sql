/**************************************
 Japan: JP_PROCEDURE → Procedure_occurrence
 procedure_code, date_of_procedure, standardized_procedure_code
 procedure_date NOT NULL — 시술일 NULL 이면 visit_start_date / visit_end_date 로 보완

 [Mapping policy]
  - 매칭용 정규화: TRIM + UPPER(norm_proc_code). 하이픈 제거(norm_proc_nohyp).
  - 슬래시 복합 코드(예: F000/F100): 앞·뒤 토큰 각각 stm 조회(하이픈 제거 변형 포함).
  - JP_PROCEDURE_MASTER 조인(코드 설명/표준화/ICD9CM 레벨 보유):
      - icd9cm_level3 → level2 → level1 를 추가 후보로 stm 조회 (있으면 큰 폭으로 cnt0 감소 가능)
      - standardized_procedure_code 도 norm_proc_code 후보로 함께 사용
  - source_to_concept_map: domain Procedure 우선 + 타깃 vocabulary CPT4/HCPCS/ICD10PCS/ICD9Proc/SNOMED/OPCS4 등
  - CDM 에 JMDC·MEDIS 등 일본 계열 vocabulary 가 적재돼 있을 때만: CONCEPT 직접 조회 폴백(Procedure 도메인)
  - 도메인 폴백(Observation 등)은 넣지 않음.

 -----------------------------------------------------------------------------
 [검증 기록 · 미매핑 비율 — 적재 후 SSMS 에서 재확인 권장]
   SELECT SUM(CASE WHEN procedure_concept_id = 0 THEN 1 ELSE 0 END) AS cnt0, COUNT(*) AS total_cnt
   FROM dbo.procedure_occurrence;

   참고(2026-03 사용자 보고):
   - 초기(LEFT JOIN + procedure domain 만): total_cnt ≈ 14,975,102 / cnt0 ≈ 11,250,049 → 미매핑 약 75.1%
   - 정규화·vocabulary 확장 후: total_cnt ≈ 14,975,082 / cnt0 ≈ 10,920,546 → 미매핑 약 72.9%

 [원인(코드 문자열 관점)]
  - 원천은 일본 레セプト 형식(A001, F400, B011-3, ZZ01, F000/F100 등). Athena 기본 Maps to 기반 stm 에
    동일 문자열이 없으면 0. 일본 고유 코드는 표준 용어에 없는 경우가 많아 ETL 만으로 한계가 큼.

 [추가로 줄이는 방법(ETL 외)]
  - 일본 코드 → concept_id 교차표를 별도 테이블로 두고 우선 조인.

 성능: NOT EXISTS용 IX_po_etl_dedup_japan. map 은 IX_stcm_source_code_japan_etl. CONCEPT 는 100에서 만든 IX_concept_voc_code_japan_etl.
**************************************/

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes i
    INNER JOIN sys.tables t ON i.object_id = t.object_id
    WHERE i.name = N'IX_po_etl_dedup_japan'
      AND t.name = N'procedure_occurrence'
      AND SCHEMA_NAME(t.schema_id) = N'dbo'
)
    CREATE NONCLUSTERED INDEX IX_po_etl_dedup_japan
        ON dbo.procedure_occurrence (person_id, visit_occurrence_id, procedure_source_value, procedure_date);
GO

INSERT INTO @cdm_database.procedure_occurrence (
    procedure_occurrence_id, person_id, procedure_concept_id, procedure_date, procedure_datetime,
    procedure_type_concept_id, visit_occurrence_id, procedure_source_value, procedure_source_concept_id, master_seq
)
SELECT
    (SELECT ISNULL(MAX(procedure_occurrence_id), 0) FROM @cdm_database.procedure_occurrence)
    + ROW_NUMBER() OVER (ORDER BY pr.claim_id, pr.statement_id, pr.procedure_code) AS procedure_occurrence_id,
    p.person_id,
    COALESCE(cm_try.target_concept_id, jp_proc.concept_id, 0) AS procedure_concept_id,
    dt.proc_dt AS procedure_date,
    NULL AS procedure_datetime,
    44818517 AS procedure_type_concept_id,
    v.visit_occurrence_id,
    COALESCE(pr.procedure_code, CAST(pr.standardized_procedure_code AS VARCHAR(50))) AS procedure_source_value,
    NULL AS procedure_source_concept_id,
    sm.master_seq
FROM @raw_database.JP_PROCEDURE pr
INNER JOIN @cdm_database.person p ON p.person_source_value = pr.member_id
INNER JOIN @cdm_database.visit_occurrence v ON v.visit_source_value = pr.claim_id AND v.person_id = p.person_id
LEFT JOIN @raw_database.JP_PROCEDURE_MASTER pm
  ON pm.procedure_code = pr.procedure_code
CROSS APPLY (
    SELECT
        UPPER(LTRIM(RTRIM(COALESCE(pr.procedure_code, CAST(pr.standardized_procedure_code AS VARCHAR(50)))))) AS norm_proc_code
) nd
CROSS APPLY (
    SELECT REPLACE(nd.norm_proc_code, N'-', N'') AS norm_proc_nohyp
) nh
CROSS APPLY (
    SELECT
        CASE
            WHEN CHARINDEX(N'/', nd.norm_proc_code) > 0
            THEN NULLIF(
                UPPER(LTRIM(RTRIM(LEFT(nd.norm_proc_code, CHARINDEX(N'/', nd.norm_proc_code) - 1)))),
                N''
            )
            ELSE NULL
        END AS norm_slash1,
        CASE
            WHEN CHARINDEX(N'/', nd.norm_proc_code) > 0
            THEN NULLIF(
                UPPER(LTRIM(RTRIM(SUBSTRING(
                    nd.norm_proc_code,
                    CHARINDEX(N'/', nd.norm_proc_code) + 1,
                    50
                )))),
                N''
            )
            ELSE NULL
        END AS norm_slash2
) ns
OUTER APPLY (
    SELECT
        NULLIF(UPPER(LTRIM(RTRIM(pm.standardized_procedure_code))), N'') AS pm_std_code,
        NULLIF(UPPER(LTRIM(RTRIM(pm.icd9cm_level3))), N'') AS pm_icd9_3,
        NULLIF(UPPER(LTRIM(RTRIM(pm.icd9cm_level2))), N'') AS pm_icd9_2,
        NULLIF(UPPER(LTRIM(RTRIM(pm.icd9cm_level1))), N'') AS pm_icd9_1
) pmc
OUTER APPLY (
    SELECT TOP (1)
        m.target_concept_id
    FROM @cdm_database.source_to_concept_map m
    WHERE m.invalid_reason IS NULL
      AND (
          m.source_code = nd.norm_proc_code
          OR m.source_code = nh.norm_proc_nohyp
          OR (pmc.pm_std_code IS NOT NULL AND m.source_code IN (pmc.pm_std_code, REPLACE(pmc.pm_std_code, N'-', N'')))
          OR (pmc.pm_icd9_3 IS NOT NULL AND m.source_code = pmc.pm_icd9_3)
          OR (pmc.pm_icd9_2 IS NOT NULL AND m.source_code = pmc.pm_icd9_2)
          OR (pmc.pm_icd9_1 IS NOT NULL AND m.source_code = pmc.pm_icd9_1)
          OR (
              ns.norm_slash1 IS NOT NULL
              AND m.source_code IN (ns.norm_slash1, REPLACE(ns.norm_slash1, N'-', N''))
          )
          OR (
              ns.norm_slash2 IS NOT NULL
              AND m.source_code IN (ns.norm_slash2, REPLACE(ns.norm_slash2, N'-', N''))
          )
      )
      AND (
          LOWER(m.domain_id) = N'procedure'
          OR m.target_vocabulary_id IN (
              N'CPT4', N'HCPCS', N'ICD10PCS', N'ICD9Proc', N'SNOMED', N'OPCS4'
          )
          OR m.target_vocabulary_id LIKE N'HCPCS%'
      )
    ORDER BY
        CASE
            WHEN m.source_code = nd.norm_proc_code THEN 1
            WHEN m.source_code = nh.norm_proc_nohyp THEN 2
            WHEN pmc.pm_std_code IS NOT NULL AND m.source_code IN (pmc.pm_std_code, REPLACE(pmc.pm_std_code, N'-', N'')) THEN 3
            WHEN pmc.pm_icd9_3 IS NOT NULL AND m.source_code = pmc.pm_icd9_3 THEN 4
            WHEN pmc.pm_icd9_2 IS NOT NULL AND m.source_code = pmc.pm_icd9_2 THEN 5
            WHEN pmc.pm_icd9_1 IS NOT NULL AND m.source_code = pmc.pm_icd9_1 THEN 6
            WHEN ns.norm_slash1 IS NOT NULL AND m.source_code IN (ns.norm_slash1, REPLACE(ns.norm_slash1, N'-', N'')) THEN 3
            WHEN ns.norm_slash2 IS NOT NULL AND m.source_code IN (ns.norm_slash2, REPLACE(ns.norm_slash2, N'-', N'')) THEN 4
            ELSE 5
        END,
        CASE WHEN LOWER(m.domain_id) = N'procedure' THEN 1 ELSE 2 END,
        CASE
            WHEN m.target_vocabulary_id IN (N'CPT4', N'ICD10PCS', N'ICD9Proc', N'SNOMED', N'OPCS4') THEN 1
            WHEN m.target_vocabulary_id LIKE N'HCPCS%' OR m.target_vocabulary_id = N'HCPCS' THEN 2
            ELSE 3
        END,
        m.target_concept_id
) cm_try
-- 용어 zip 에 JMDC/MEDIS 등이 포함돼 있고 concept_code 가 일치할 때만 효과(없으면 항상 NULL)
OUTER APPLY (
    SELECT TOP (1)
        c.concept_id
    FROM @cdm_database.CONCEPT c
    WHERE (c.invalid_reason IS NULL OR c.invalid_reason = N'')
      AND LOWER(c.domain_id) = N'procedure'
      AND c.vocabulary_id IN (N'JMDC', N'MEDIS')
      AND (
          c.concept_code = nd.norm_proc_code
          OR c.concept_code = nh.norm_proc_nohyp
          OR (ns.norm_slash1 IS NOT NULL AND c.concept_code IN (ns.norm_slash1, REPLACE(ns.norm_slash1, N'-', N'')))
          OR (ns.norm_slash2 IS NOT NULL AND c.concept_code IN (ns.norm_slash2, REPLACE(ns.norm_slash2, N'-', N'')))
      )
    ORDER BY
        CASE c.vocabulary_id WHEN N'JMDC' THEN 1 ELSE 2 END,
        c.concept_id
) jp_proc
OUTER APPLY (
    SELECT TOP (1) x.master_seq
    FROM @cdm_database.SEQ_MASTER x
    WHERE x.source_table = N'PRC'
      AND x.member_id = CAST(pr.member_id AS VARCHAR(50))
      AND x.claim_id = CAST(pr.claim_id AS VARCHAR(50))
      AND (
          (x.statement_id = CAST(pr.statement_id AS FLOAT) AND pr.statement_id IS NOT NULL)
          OR (x.statement_id IS NULL AND pr.statement_id IS NULL)
      )
) sm
CROSS APPLY (
    SELECT CAST(COALESCE(
        pr.date_of_procedure,
        v.visit_start_date,
        v.visit_end_date
    ) AS DATE) AS proc_dt
) dt
WHERE dt.proc_dt IS NOT NULL
AND NOT EXISTS (
    SELECT 1 FROM @cdm_database.procedure_occurrence po
    WHERE po.person_id = p.person_id AND po.visit_occurrence_id = v.visit_occurrence_id
    AND po.procedure_source_value = COALESCE(pr.procedure_code, CAST(pr.standardized_procedure_code AS VARCHAR(50)))
    AND po.procedure_date = dt.proc_dt
);
