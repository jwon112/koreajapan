/*********************************************************************************
Japan CDM: Vocabulary 복사
@source_database: 한국 Vocabulary가 있는 DB (예: nhisnsc2013cdm)
@target_database: japan_cohort_cdm
*********************************************************************************/

USE @target_database;

-- 1. CONCEPT (컬럼 명시: 소스 스키마 차이 대응)
TRUNCATE TABLE CONCEPT;
INSERT INTO CONCEPT (concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, standard_concept, concept_code, valid_start_date, valid_end_date, invalid_reason)
SELECT concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, standard_concept, concept_code, valid_start_date, valid_end_date, invalid_reason
FROM @source_database.dbo.CONCEPT;

-- 2. CONCEPT_SYNONYM
TRUNCATE TABLE CONCEPT_SYNONYM;
INSERT INTO CONCEPT_SYNONYM (concept_id, concept_synonym_name, language_concept_id)
SELECT concept_id, concept_synonym_name, language_concept_id
FROM @source_database.dbo.CONCEPT_SYNONYM;

-- 3. CONCEPT_RELATIONSHIP
TRUNCATE TABLE CONCEPT_RELATIONSHIP;
INSERT INTO CONCEPT_RELATIONSHIP SELECT * FROM @source_database.dbo.CONCEPT_RELATIONSHIP;

-- 4. CONCEPT_ANCESTOR
TRUNCATE TABLE CONCEPT_ANCESTOR;
INSERT INTO CONCEPT_ANCESTOR SELECT * FROM @source_database.dbo.CONCEPT_ANCESTOR;

-- 5. DRUG_STRENGTH
TRUNCATE TABLE DRUG_STRENGTH;
INSERT INTO DRUG_STRENGTH SELECT * FROM @source_database.dbo.DRUG_STRENGTH;

-- 6. VOCABULARY
TRUNCATE TABLE VOCABULARY;
INSERT INTO VOCABULARY SELECT * FROM @source_database.dbo.VOCABULARY;

-- 7. DOMAIN
TRUNCATE TABLE DOMAIN;
INSERT INTO DOMAIN SELECT * FROM @source_database.dbo.DOMAIN;

-- 8. CONCEPT_CLASS
TRUNCATE TABLE CONCEPT_CLASS;
INSERT INTO CONCEPT_CLASS SELECT * FROM @source_database.dbo.CONCEPT_CLASS;

-- 9. RELATIONSHIP
TRUNCATE TABLE RELATIONSHIP;
INSERT INTO RELATIONSHIP SELECT * FROM @source_database.dbo.RELATIONSHIP;

-- 10. source_to_concept_map
TRUNCATE TABLE source_to_concept_map;
INSERT INTO source_to_concept_map SELECT * FROM @source_database.dbo.source_to_concept_map;
