/**************************************
 Japan: Care_site (minimal)
 - JP_* 테이블의 medical_facility_id를 care_site_source_value로 사용.
 - place_of_service_concept_id 는 정보 부족으로 0.
 - location_id 는 원천에 지역 정보가 없어 NULL 유지.
**************************************/

;WITH raw_facility AS (
    SELECT DISTINCT NULLIF(LTRIM(RTRIM(medical_facility_id)), '') AS medical_facility_id
    FROM @raw_database.JP_CLAIMS
    UNION
    SELECT DISTINCT NULLIF(LTRIM(RTRIM(medical_facility_id)), '') AS medical_facility_id
    FROM @raw_database.JP_DIAGNOSIS
    UNION
    SELECT DISTINCT NULLIF(LTRIM(RTRIM(medical_facility_id)), '') AS medical_facility_id
    FROM @raw_database.JP_DRUG
    UNION
    SELECT DISTINCT NULLIF(LTRIM(RTRIM(medical_facility_id)), '') AS medical_facility_id
    FROM @raw_database.JP_PROCEDURE
),
to_insert AS (
    SELECT rf.medical_facility_id
    FROM raw_facility rf
    WHERE rf.medical_facility_id IS NOT NULL
      AND NOT EXISTS (
          SELECT 1
          FROM @cdm_database.care_site cs
          WHERE cs.care_site_source_value = rf.medical_facility_id
      )
)
INSERT INTO @cdm_database.care_site (
    care_site_id, care_site_name, place_of_service_concept_id, location_id,
    care_site_source_value, place_of_service_source_value
)
SELECT
    (SELECT ISNULL(MAX(care_site_id), 0) FROM @cdm_database.care_site)
    + ROW_NUMBER() OVER (ORDER BY t.medical_facility_id) AS care_site_id,
    NULL AS care_site_name,
    0 AS place_of_service_concept_id,
    NULL AS location_id,
    t.medical_facility_id AS care_site_source_value,
    NULL AS place_of_service_source_value
FROM to_insert t;

-- visit_occurrence.care_site_id 채우기 (가능한 범위에서만)
UPDATE v
SET v.care_site_id = cs.care_site_id
FROM @cdm_database.visit_occurrence v
JOIN @raw_database.JP_CLAIMS c
  ON c.claim_id = v.visit_source_value
JOIN @cdm_database.care_site cs
  ON cs.care_site_source_value = c.medical_facility_id
WHERE v.care_site_id IS NULL;

