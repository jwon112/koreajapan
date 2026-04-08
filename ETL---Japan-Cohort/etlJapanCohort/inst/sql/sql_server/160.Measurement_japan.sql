/**************************************
 Japan: Measurement (no source yet)
 - 현재 japan_cohort_raw 스키마에는 measurement로 변환할 검사/측정 원천 테이블(검사코드, 결과값, 단위, 검사일 등)이 없어 적재하지 않음(0행 유지).
 - 원천 테이블이 추가되면 이 파일에 매핑 로직을 구현할 것.
**************************************/

INSERT INTO @cdm_database.measurement (
    measurement_id, person_id, measurement_concept_id, measurement_date, measurement_datetime, measurement_time,
    measurement_type_concept_id, operator_concept_id, value_as_number, value_as_concept_id, unit_concept_id,
    range_low, range_high, provider_id, visit_occurrence_id, visit_detail_id,
    measurement_source_value, measurement_source_concept_id, unit_source_value, value_source_value
)
SELECT
    NULL, NULL, 0, CAST('1900-01-01' AS DATE), NULL, NULL,
    0, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL,
    NULL, NULL, NULL, NULL
FROM (SELECT 1 AS x) d
WHERE 1 = 0;

