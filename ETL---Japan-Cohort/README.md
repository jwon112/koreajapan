# ETL --- Japan Cohort

일본 cohort 원본 데이터 → OMOP CDM 변환 ETL  
**Python 기반** (편의성, novelty)

## 디렉터리 구조

```
ETL---Japan-Cohort/
└── etlJapanCohort/
    ├── inst/
    │   ├── sql/sql_server/        # 000, 001, ... (한국과 동일 구조)
    │   └── mapping_table/
    ├── scripts/
    │   ├── load_sas_to_sql_japan.py   # SAS → japan_cohort_raw
    │   ├── phase0_setup.py            # DB + DDL
    │   ├── execute_japan_etl.py       # run_japan_etl(flags) — 플래그는 codeToRun_japan.py / ipynb
    │   ├── import_vocabulary_copy.py  # Vocabulary 복사
    │   └── import_vocabulary.py       # Vocabulary BULK INSERT
    └── extras/
        ├── PRE_EXECUTION_CHECKLIST.md
        └── pre_execution_check.sql
```

## 실행 (루트의 단일 진입점)

```bash
python codeToRun_japan.py [step]
```

| step | 설명 |
|------|------|
| `load_sas` | SAS → japan_cohort_raw |
| `phase0` | DB + DDL |
| `vocabulary_copy` | 한국 DB → japan_cohort_cdm 복사 |
| `vocabulary_bulk` | CSV BULK INSERT |
| `etl` | 루트 `codeToRun_japan.py` 상단 TRUE/FALSE 로 run_japan_etl |
| `all` (기본) | import_vocabulary_* True 이면 용어 적재 후 run_japan_etl |

플래그 수정: `codeToRun_japan.py` 또는 `codeToRun_japan.ipynb` 의 실행 단계 제어 / 데이터 적재 / 후처리 블록.

**한국 ETL 단계 대응**: Phase1(도메인 적재) 이후에는 generateEra, indexing, constraints 등이 있으며, 일본은 동일 플래그만 두고 SQL은 추후 확장 가능.

또는 scripts 직접: `python ETL---Japan-Cohort/etlJapanCohort/scripts/import_vocabulary_copy.py`

## 요구사항

- Python 3.8+
- pyodbc, sqlalchemy, pandas, pyreadstat
- SQL Server ODBC Driver
