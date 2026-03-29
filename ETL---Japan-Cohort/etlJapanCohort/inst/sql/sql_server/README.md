# SQL Server 스크립트

**한국(etlKoreanNSC)과 동일한 구조** - inst/sql/sql_server/ 에 SQL 보관, 실행만 Python

## 파일 구성 (한국과 동일 번호 규칙)

| 번호 | SQL | 설명 |
|------|-----|------|
| 000 | OMOP CDM sql server ddl | CDM 테이블 DDL (Vocabulary 포함, @cdm_database) |
| 001 | Import_voca | BULK INSERT (@Mapping_database, @vocaFolder) |
| 001 | Import_vocabulary_copy | Japan 전용: 한국 DB → 복사 |
| 010~900 | Master_table, Location, ... | 향후 추가 (한국과 동일 번호) |

## 실행

Python scripts가 `inst/sql/sql_server/`에서 SQL 로드 → 파라미터 치환 → 실행
