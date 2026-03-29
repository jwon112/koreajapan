/*********************************************************************************
 Japan: SEQ_MASTER (한국 NHIS 010.Master_table 개념)
 - claim / statement 단위 키 통합용 확장 테이블
 - 현재 Phase1 JP SQL은 visit_occurrence_id를 claim_id 기반으로 직접 사용.
   향후 master_seq 기반으로 바꿀 경우 이 테이블을 채움.
*********************************************************************************/

IF OBJECT_ID('@cdm_database.SEQ_MASTER', 'U') IS NOT NULL
    DROP TABLE @cdm_database.SEQ_MASTER;

CREATE TABLE @cdm_database.SEQ_MASTER (
    master_seq          BIGINT IDENTITY(1, 1) NOT NULL PRIMARY KEY,
    source_table        VARCHAR(10) NOT NULL,
    member_id           VARCHAR(50) NOT NULL,
    claim_id            VARCHAR(50) NULL,
    statement_id        FLOAT NULL
);

-- 필요 시 진단/약제/시술 행별 INSERT 예시 (주석)
-- INSERT INTO @cdm_database.SEQ_MASTER (source_table, member_id, claim_id, statement_id)
-- SELECT 'DIA', member_id, claim_id, statement_id FROM @raw_database.JP_DIAGNOSIS;
