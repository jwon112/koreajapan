# Phase 1 스키마 매핑 (실제 스키마 반영)

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
| condition_start_date | date_of_medical_care_start | |
| condition_type_concept_id | main_disease_flag | 1=44786627(primary), else 44786629 |

## JP_DRUG → drug_exposure

| CDM | 원본 컬럼 | 설명 |
|-----|-----------|------|
| drug_source_value | who_atc_code, jmdc_drug_code | ATC 우선 |
| drug_exposure_start_date | date_of_prescription | |
| days_supply | administered_days | |
| quantity | administered_amount | |

## JP_PROCEDURE → procedure_occurrence

| CDM | 원본 컬럼 | 설명 |
|-----|-----------|------|
| procedure_source_value | procedure_code, standardized_procedure_code | procedure_code 우선 |
| procedure_date | date_of_procedure | |
