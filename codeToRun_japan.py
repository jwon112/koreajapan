"""
Japan CDM ETL 진입점 (한국 codeToRun_2.R 과 동일하게 아래에서 TRUE/FALSE 수정 후 반복 실행)

사용: python codeToRun_japan.py [step]

  step:
    load_sas            SAS → japan_cohort_raw
    phase0              DB 생성 + 000 DDL (전체, 플래그 무관)
    vocabulary_copy     한국 DB → japan_cohort_cdm (단독)
    vocabulary_bulk       CSV BULK (단독)
    etl                 아래 플래그로 run_japan_etl + (선택) 용어 적재
    all (기본)          import_vocabulary_* 가 True 이면 용어 적재 후 run_japan_etl
"""

import os
import sys
import subprocess

ROOT = os.path.dirname(os.path.abspath(__file__))
SCRIPTS = os.path.join(ROOT, "ETL---Japan-Cohort", "etlJapanCohort", "scripts")


# =============================================================================
# --- 실행 단계 제어 (TRUE/FALSE) ---
# 처음 돌릴 때는 CDM_ddl = True 로 빈 CDM 테이블을 만들어야 합니다.
# =============================================================================
CDM_ddl = False
create_database_if_missing = True
master_table = False
import_vocabulary_copy = False
import_vocabulary_bulk = False

# =============================================================================
# --- 데이터 적재 (필요한 부분만 TRUE) ---
# 실행 순서는 phase1_setup.ETL_STEP_SQL 고정 (여러 개 True여도 동일):
#   person → death → observation_period → visit_occurrence → condition_occurrence
#   → drug_exposure → procedure_occurrence
# =============================================================================
# --- Phase1 도메인 ---
person = False
death = False
observation_period = False
visit_occurrence = False
condition_occurrence = False
drug_exposure = False
procedure_occurrence = False

# --- 확장 도메인 (일부는 '소스 없음'으로 0행이 정상) ---
# [권장 순서(의존성)]
#   visit_occurrence → care_site
#   person → payer_plan_period
#   (visit_occurrence, drug_exposure, procedure_occurrence) → cost
#
# [현재 구현 상태(일본 RAW 스키마 기준)]
# - 실제 적재됨: care_site, payer_plan_period, cost
# - 0행이 정상(no-op): location(주소/지역 소스 없음), observation/measurement/device_exposure(원천 테이블 없음)
location = False
care_site = False
observation = False
device_exposure = False
measurement = False
payer_plan_period = False
cost = False

# =============================================================================
# --- 후처리 ---
# =============================================================================
generateEra = False
dose_era = False
cdm_source = False
indexing = False
constraints = False
data_cleansing = False


def _collect_flags():
    return {
        "CDM_ddl": CDM_ddl,
        "create_database_if_missing": create_database_if_missing,
        "master_table": master_table,
        "person": person,
        "death": death,
        "observation_period": observation_period,
        "visit_occurrence": visit_occurrence,
        "condition_occurrence": condition_occurrence,
        "drug_exposure": drug_exposure,
        "procedure_occurrence": procedure_occurrence,
        "location": location,
        "care_site": care_site,
        "observation": observation,
        "device_exposure": device_exposure,
        "measurement": measurement,
        "payer_plan_period": payer_plan_period,
        "cost": cost,
        "generateEra": generateEra,
        "dose_era": dose_era,
        "cdm_source": cdm_source,
        "indexing": indexing,
        "constraints": constraints,
        "data_cleansing": data_cleansing,
    }


def run(script_name):
    path = os.path.join(SCRIPTS, script_name)
    if not os.path.exists(path):
        print(f"[ERROR] Not found: {path}")
        return 1
    return subprocess.call([sys.executable, path], cwd=ROOT)


def run_vocabulary_imports():
    if import_vocabulary_copy:
        print("\n--- import_vocabulary_copy ---\n")
        if run("import_vocabulary_copy.py") != 0:
            return 1
    if import_vocabulary_bulk:
        print("\n--- import_vocabulary (bulk) ---\n")
        if run("import_vocabulary.py") != 0:
            return 1
    return 0


def run_etl_with_flags():
    sys.path.insert(0, SCRIPTS)
    from execute_japan_etl import run_japan_etl
    return run_japan_etl(_collect_flags())


def _etl_step():
    if run_vocabulary_imports() != 0:
        return 1
    return run_etl_with_flags()


def main():
    step = (sys.argv[1] if len(sys.argv) > 1 else "all").lower()
    steps = {
        "load_sas": lambda: run("load_sas_to_sql_japan.py"),
        "phase0": lambda: run("phase0_setup.py"),
        "vocabulary_copy": lambda: run("import_vocabulary_copy.py"),
        "vocabulary_bulk": lambda: run("import_vocabulary.py"),
        "etl": _etl_step,
    }

    if step not in steps and step != "all":
        print(__doc__)
        print("Available steps:", ", ".join(steps) + ", all")
        return 1

    if step == "all":
        if run_vocabulary_imports() != 0:
            return 1
        return run_etl_with_flags()

    return steps[step]()


if __name__ == "__main__":
    sys.exit(main())
