"""
Phase 1: japan_cohort_raw → japan_cohort_cdm ETL (도메인별 SQL)
- run_japan_etl(flags) 에서 넘긴 dict 로 실행할 스크립트 선택
- execute_japan_etl.py 에서 호출
"""

from sqlalchemy import create_engine, text
import os
import re
import urllib

server = r"DESKTOP-HBA9S76\SQLEXPRESS01"
database_raw = "japan_cohort_raw"
database_cdm = "japan_cohort_cdm"
use_windows_auth = True

_script_dir = os.path.dirname(os.path.abspath(__file__))
sql_dir = os.path.normpath(os.path.join(_script_dir, "..", "inst", "sql", "sql_server"))

# 플래그 이름 → SQL 파일 (실행 순서 고정)
ETL_STEP_SQL = [
    ("person", "040.Person_japan.sql"),
    ("death", "050.Death_japan.sql"),
    ("observation_period", "060.Observation_period_japan.sql"),
    ("visit_occurrence", "070.Visit_occurrence_japan.sql"),
    ("condition_occurrence", "080.Condition_occurrence_japan.sql"),
    ("drug_exposure", "100.Drug_exposure_japan.sql"),
    ("procedure_occurrence", "110.Procedure_occurrence_japan.sql"),
]

MASTER_SQL = "010.Master_table_japan.sql"


def _engine_cdm():
    if use_windows_auth:
        conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={server};DATABASE={database_cdm};"
            f"Trusted_Connection=Yes;Encrypt=no;"
        )
    else:
        conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={server};DATABASE={database_cdm};"
            f"UID=sa;PWD=KoreaJapan44@;Encrypt=no;"
        )
    params = urllib.parse.quote_plus(conn_str)
    return create_engine(
        f"mssql+pyodbc:///?odbc_connect={params}",
        isolation_level="AUTOCOMMIT"
    )


def load_sql_batches(path):
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        content = f.read()
    content = content.replace("@raw_database", f"{database_raw}.dbo")
    content = content.replace("@cdm_database", f"{database_cdm}.dbo")
    # GO 배치 분리 (SQL Server)
    batches = [b.strip() for b in re.split(r"^\s*GO\s*$", content, flags=re.MULTILINE) if b.strip()]
    return batches


def run_sql_file(engine, sql_path, label):
    if not os.path.exists(sql_path):
        print(f"  [SKIP] Not found: {label}")
        return
    print(f"  Running {label}...")
    batches = load_sql_batches(sql_path)
    with engine.connect() as conn:
        for batch in batches:
            if batch and not batch.strip().startswith("--"):
                conn.execute(text(batch))
    print(f"  Done: {label}")


def run_master_table():
    path = os.path.join(sql_dir, MASTER_SQL)
    run_sql_file(_engine_cdm(), path, MASTER_SQL)


def run_etl_steps(flags):
    """
    flags: dict (또는 속성으로 True/False 인 객체)
    """
    engine = _engine_cdm()
    for key, sql_name in ETL_STEP_SQL:
        val = flags.get(key) if isinstance(flags, dict) else getattr(flags, key, False)
        if not val:
            continue
        path = os.path.join(sql_dir, sql_name)
        run_sql_file(engine, path, sql_name)


if __name__ == "__main__":
    # 하위 호환: 기본은 전부 실행
    import types
    flags = {k: True for k, _ in ETL_STEP_SQL}
    print("\n--- Phase 1: all ETL steps (legacy __main__) ---\n")
    run_etl_steps(flags)
    print("\n[Phase 1 Complete]")
