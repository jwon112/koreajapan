# Phase 1 스키마 매핑 (실제 스키마 반영)

## SEQ_MASTER (중간 타협 — 원천 추적)

`010.Master_table_japan.sql`이 원천 행을 적재하고, CDM에 **`master_seq` 컬럼**(nullable BIGINT)을 추가한 뒤 도메인 INSERT에서 조인한다.

| source_table | 원천 테이블 | grain | CDM 연결 |
|--------------|-------------|-------|----------|
| CLM | JP_CLAIMS | 청구(claim) 1행 | `visit_occurrence.master_seq` |
| DIA | JP_DIAGNOSIS | 진단 명세 1행 | `condition_occurrence.master_seq` |
| DRG | JP_DRUG | 약제 명세 1행 | `drug_exposure.master_seq` |
| PRC | JP_PROCEDURE | 시술 명세 1행 | `procedure_occurrence.master_seq` |

**실행 순서**: `010` 마스터 적재 → `070` 방문 → `080`/`100`/`110` (마스터 없이 도메인만 돌리면 `master_seq` 컬럼이 없을 수 있음 — 반드시 먼저 `master_table` 실행).

**재실행**: `SEQ_MASTER`를 DROP 후 재생성하면 `master_seq` 값이 바뀐다. 도메인 테이블을 비우고 처음부터 다시 적재하는 것을 권장.

## JP_PATIENT → person, death, observation_period

| CDM | 원본 컬럼 | 설명 |
|-----|-----------|------|
| person_id | member_id | surrogate key 생성, person_source_value에 member_id 저장 |
| gender_concept_id | gender_of_member | 1/M/Male=8507, 2/F/Female=8532 |
| year_of_birth | month_and_year_of_birth_of_memb | YYYYMM → year 추출 |
| observation_period | observation_start, observation_end | YYYYMM 또는 YYYYMMDD |
| death | withdrawal_death_flag, observation_end | 사망 시 observation_end를 death_date로 사용 |

## JP_CLAIMS → visit_occurrence

| CDM | 원본 컬럼 | 설명 |
|-----|-----------|------|
| visit_occurrence_id | claim_id | ROW_NUMBER로 bigint 생성 |
| visit_concept_id | type_of_claim | inpatient=9201, outpatient=9202 |
| visit_start_date | admission_date, discharge_date, month_and_year_of_medical_care | 우선순위 적용 |

## JP_DIAGNOSIS → condition_occurrence

| CDM | 원본 컬럼 | 설명 |
|-----|-----------|------|
| condition_source_value | icd10_level4_code | ICD-10 코드 |
| condition_start_date | date_of_medical_care_start → 없으면 visit_start_date → visit_end_date | NOT NULL 제약 |
| condition_type_concept_id | main_disease_flag | 1=44786627(primary), else 44786629 |
| condition_concept_id | `source_to_concept_map` | ETL에서 ICD10 코드 정규화 후 매칭: `-` 제거 + 필요 시 dot 삽입(K291→K29.1) |

## JP_DRUG → drug_exposure

| CDM | 원본 컬럼 | 설명 |
|-----|-----------|------|
| drug_source_value | who_atc_code, jmdc_drug_code | ATC 우선 |
| drug_exposure_start_date | date_of_prescription → 없으면 visit_start_date / visit_end_date | NOT NULL 제약 |
| days_supply | administered_days | |
| quantity | administered_amount | |

## JP_PROCEDURE → procedure_occurrence

| CDM | 원본 컬럼 | 설명 |
|-----|-----------|------|
| procedure_source_value | procedure_code, standardized_procedure_code | procedure_code 우선 |
| procedure_date | date_of_procedure → 없으면 visit_start_date / visit_end_date | NOT NULL 제약 |
