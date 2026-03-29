# 일본 CDM ETL - 실행 전 확인 사항

**실행 순서**: 아래 1~3 확인 후 `python scripts/phase0_setup.py` 실행  
(실행 위치: `ETL---Japan-Cohort/etlJapanCohort/` 또는 프로젝트 루트)

## 1. 데이터베이스

- [ ] `japan_cohort_raw` DB 존재
- [ ] 8개 테이블 모두 적재됨 (JP_PATIENT, JP_CLAIMS, JP_DIAGNOSIS, JP_DRUG, JP_PROCEDURE, JP_*_MASTER)
- [ ] 행 수 확인 완료

```sql
SELECT name FROM sys.databases WHERE name IN ('japan_cohort_raw', 'japan_cohort_cdm');
SELECT 'JP_PATIENT' t, COUNT(*) FROM japan_cohort_raw.dbo.JP_PATIENT
UNION ALL SELECT 'JP_CLAIMS', COUNT(*) FROM japan_cohort_raw.dbo.JP_CLAIMS
UNION ALL SELECT 'JP_DIAGNOSIS', COUNT(*) FROM japan_cohort_raw.dbo.JP_DIAGNOSIS;
```

## 2. Vocabulary (둘 중 하나)

**복사 방식** (권장):  
- [ ] 한국 DB `nhisnsc2013cdm`에 Vocabulary 테이블 존재

**BULK INSERT 방식**:  
- [ ] `C:\Users\chaeyoon\Desktop\koreajapan\vocabulary` 폴더에 CONCEPT.csv 등 필수 파일 존재

## 3. SQL Server

- [ ] 인스턴스 실행 중: `DESKTOP-HBA9S76\SQLEXPRESS01` (또는 사용 환경에 맞게)
- [ ] 접속 권한 (DB 생성, 테이블 생성, BULK INSERT)

## 4. Phase 0 실행 후

- [ ] `japan_cohort_cdm` DB 생성됨
- [ ] CDM 테이블(person, visit_occurrence, condition_occurrence 등) 생성됨
- [ ] Vocabulary 테이블(concept, concept_relationship 등) 데이터 적재됨

