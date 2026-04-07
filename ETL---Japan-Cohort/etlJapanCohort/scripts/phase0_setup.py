"""
Phase 0: japan_cohort_cdm DB 생성 및 CDM DDL 적용
- 실행 전 extras/PRE_EXECUTION_CHECKLIST.md 및 pre_execution_check.sql 확인
- execute_japan_etl.py 에서 CDM_ddl 플래그로 DDL 생략 가능
"""

from sqlalchemy import create_engine, text
import os
import time
import urllib

server = r"DESKTOP-HBA9S76\SQLEXPRESS01"
database_master = "master"
database_cdm = "japan_cohort_cdm"
use_windows_auth = True

_script_dir = os.path.dirname(os.path.abspath(__file__))
ddl_path = os.path.normpath(os.path.join(
    _script_dir, "..", "inst", "sql", "sql_server",
    "000.OMOP CDM sql server ddl.sql"
))


def _conn_str_master():
    if use_windows_auth:
        return (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={server};DATABASE={database_master};"
            f"Trusted_Connection=Yes;Encrypt=no;"
        )
    return (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={server};DATABASE={database_master};"
        f"UID=sa;PWD=KoreaJapan44@;Encrypt=no;"
    )


def run_phase0(cdm_ddl=True, create_database_if_missing=True):
    """
    create_database_if_missing: True 이면 japan_cohort_cdm 없으면 CREATE DATABASE
    cdm_ddl: True 이면 000 DDL 실행 (빈 CDM 테이블 생성)
    """
    conn_str = _conn_str_master()
    params = urllib.parse.quote_plus(conn_str)
    engine_master = create_engine(
        f"mssql+pyodbc:///?odbc_connect={params}",
        isolation_level="AUTOCOMMIT"
    )

    if create_database_if_missing:
        with engine_master.connect() as conn:
            r = conn.execute(text(f"SELECT DB_ID('{database_cdm}')"))
            if r.scalar() is None:
                print(f"[Phase0] Creating database '{database_cdm}'...")
                conn.execute(text(f"CREATE DATABASE {database_cdm}"))
                print("      Done.")
            else:
                print(f"[Phase0] Database '{database_cdm}' already exists.")

    if not cdm_ddl:
        print("[Phase0] CDM_ddl=False - skipping 000 DDL.")
        return

    print("[Phase0] Loading CDM DDL...")
    if not os.path.exists(ddl_path):
        raise FileNotFoundError(f"DDL not found: {ddl_path}")

    with open(ddl_path, "r", encoding="utf-8", errors="replace") as f:
        ddl_content = f.read()
    ddl_content = ddl_content.replace("@cdm_database", database_cdm)

    conn_str_cdm = conn_str.replace(f"DATABASE={database_master}", f"DATABASE={database_cdm}")
    params_cdm = urllib.parse.quote_plus(conn_str_cdm)
    engine_cdm = create_engine(
        f"mssql+pyodbc:///?odbc_connect={params_cdm}",
        isolation_level="AUTOCOMMIT"
    )

    print("[Phase0] Executing DDL (may take several minutes)...", flush=True)
    t0 = time.perf_counter()
    with engine_cdm.connect() as conn:
        try:
            conn.execute(text(ddl_content))
            print(
                f"      DDL executed successfully.  ({time.perf_counter() - t0:.1f}s)",
                flush=True,
            )
        except Exception as e:
            err = str(e)
            if "already exists" in err.lower() or "이미 있습니다" in err:
                print(f"      [WARN] Some objects may already exist: {err[:200]}...")
            else:
                raise

    print("[Phase0] Complete.")


if __name__ == "__main__":
    run_phase0(cdm_ddl=True, create_database_if_missing=True)
