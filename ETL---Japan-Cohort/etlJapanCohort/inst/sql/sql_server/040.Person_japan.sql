/**************************************
 Japan: JP_PATIENT → PERSON
 member_id(varchar)→person_id, month_and_year_of_birth_of_memb(YYYYMM), gender_of_member
**************************************/

INSERT INTO @cdm_database.person (
    person_id, gender_concept_id, year_of_birth, month_of_birth, day_of_birth,
    birth_datetime, race_concept_id, ethnicity_concept_id, location_id, provider_id,
    care_site_id, person_source_value, gender_source_value, gender_source_concept_id,
    race_source_value, race_source_concept_id, ethnicity_source_value, ethnicity_source_concept_id
)
SELECT
    (SELECT ISNULL(MAX(person_id), 0) FROM @cdm_database.person)
    + ROW_NUMBER() OVER (ORDER BY p.member_id) AS person_id,
    CASE
        WHEN p.gender_of_member IN ('1', 'M', 'Male', 'male') THEN 8507
        WHEN p.gender_of_member IN ('2', 'F', 'Female', 'female') THEN 8532
        ELSE 0
    END AS gender_concept_id,
    CASE
        WHEN LEN(RTRIM(p.month_and_year_of_birth_of_memb)) >= 4
        THEN TRY_CAST(SUBSTRING(RTRIM(p.month_and_year_of_birth_of_memb), 1, 4) AS INT)
        ELSE 1950
    END AS year_of_birth,
    CASE
        WHEN LEN(RTRIM(p.month_and_year_of_birth_of_memb)) >= 6
        THEN TRY_CAST(SUBSTRING(RTRIM(p.month_and_year_of_birth_of_memb), 5, 2) AS INT)
        ELSE NULL
    END AS month_of_birth,
    NULL AS day_of_birth,
    NULL AS birth_datetime,
    38003585 AS race_concept_id,
    38003564 AS ethnicity_concept_id,
    NULL AS location_id,
    NULL AS provider_id,
    NULL AS care_site_id,
    p.member_id AS person_source_value,
    p.gender_of_member AS gender_source_value,
    NULL AS gender_source_concept_id,
    NULL AS race_source_value,
    NULL AS race_source_concept_id,
    NULL AS ethnicity_source_value,
    NULL AS ethnicity_source_concept_id
FROM @raw_database.JP_PATIENT p
WHERE p.member_id NOT IN (SELECT person_source_value FROM @cdm_database.person);
