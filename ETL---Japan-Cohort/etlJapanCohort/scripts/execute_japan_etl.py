"""
일본 CDM 실행 오케스트레이터 (한국 codeToRun_2.R / executeNHISETL 유사)
- 플래그는 codeToRun_japan.py 또는 codeToRun_japan.ipynb 에서 dict 로 넘김

실행 순서:
  1) CDM_ddl / DB 생성
  2) master_table (010.Master_table_japan.sql)
  3) 도메인 ETL (person, visit_occurrence, …)
  4) 후처리 플래그는 일본용 SQL 없으면 로그만

직접 실행 시: python execute_japan_etl.py  → 빈 플래그 (아무 것도 안 함).
  메인 진입점(codeToRun_japan.py)에서 플래그를 지정하세요.
"""

import importlib
import os
import sys

_script_dir = os.path.dirname(os.path.abspath(__file__))
if _script_dir not in sys.path:
    sys.path.insert(0, _script_dir)

import time

import phase0_setup
import phase1_setup

POST_ETL_FLAGS = (
    "generateEra",
    "dose_era",
    "cdm_source",
    "indexing",
    "constraints",
    "data_cleansing",
)


def _get(f, key, default=False):
    if isinstance(f, dict):
        return bool(f.get(key, default))
    return bool(getattr(f, key, default))


def run_japan_etl(flags):
    """
    flags: dict 또는 SimpleNamespace — codeToRun_2.R 의 인자와 동일한 키 사용

    Jupyter 등에서 스크립트를 수정한 뒤에도 반영되도록 phase0/phase1 모듈을 매번 reload 함.
    """
    importlib.reload(phase0_setup)
    importlib.reload(phase1_setup)
    run_phase0 = phase0_setup.run_phase0
    ETL_STEP_SQL = phase1_setup.ETL_STEP_SQL
    run_master_table = phase1_setup.run_master_table
    run_etl_steps = phase1_setup.run_etl_steps

    print("\n=== run_japan_etl ===\n")

    create_db = _get(flags, "create_database_if_missing", True)
    cdm_ddl = _get(flags, "CDM_ddl", False)

    if create_db or cdm_ddl:
        run_phase0(cdm_ddl=cdm_ddl, create_database_if_missing=create_db)
    else:
        print("[Skip] create_database_if_missing=False and CDM_ddl=False\n")

    if _get(flags, "master_table", False):
        print("[master_table]", flush=True)
        t0 = time.perf_counter()
        run_master_table()
        print(f"[master_table] finished in {time.perf_counter() - t0:.1f}s\n", flush=True)

    any_etl = any(_get(flags, key, False) for key, _ in ETL_STEP_SQL)
    if any_etl:
        print("[Domain ETL]")
        run_etl_steps(flags)
        print()

    for name in POST_ETL_FLAGS:
        if _get(flags, name, False):
            print(f"  [TODO] {name}=True - 일본용 SQL 미구현, 스킵.")

    print("=== run_japan_etl done ===\n")
    return 0


def main():
    print("플래그 없음: codeToRun_japan.py 상단 변수 또는 codeToRun_japan.ipynb 에서 run_japan_etl(flags) 호출.\n")
    return run_japan_etl({})  # 내부에서 phase 모듈 reload


if __name__ == "__main__":
    sys.exit(main())
