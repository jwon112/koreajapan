"""
001.Import_vocabulary_copy.sql 실행 (한국 DB → japan_cohort_cdm 복사)
- SQL: inst/sql/sql_server/ (한국에 없는 Japan 전용)
- Python: 파라미터 치환 후 실행만
"""

import os
import urllib

from sqlalchemy import create_engine, text

# ------------------------------------------------------
# 설정
# ------------------------------------------------------

server = r"DESKTOP-HBA9S76\SQLEXPRESS01"
database_source = "nhisnsc2013cdm"
database_target = "japan_cohort_cdm"
use_windows_auth = True

_script_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.normpath(os.path.join(_script_dir, "..", "..", ".."))
# ------------------------------------------------------
# 연결
# ------------------------------------------------------

if use_windows_auth:
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={server};DATABASE={database_target};"
        f"Trusted_Connection=Yes;Encrypt=no;"
    )
else:
    username, password = "sa", "KoreaJapan44@"
    conn_str = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={server};DATABASE={database_target};"
        f"UID={username};PWD={password};Encrypt=no;"
    )

# CONCEPT 10M행 복사 시 타임아웃 방지
params = urllib.parse.quote_plus(conn_str + ";Connection Timeout=600")
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}", isolation_level="AUTOCOMMIT")

# ------------------------------------------------------
# SQL 로드 → 치환 → 실행
# ------------------------------------------------------

if __name__ == "__main__":
    sql_path = os.path.normpath(os.path.join(_script_dir, "..", "inst", "sql", "sql_server", "001.Import_vocabulary_copy.sql"))
    if not os.path.exists(sql_path):
        raise FileNotFoundError(f"SQL not found: {sql_path}")

    with open(sql_path, "r", encoding="utf-8") as f:
        sql_content = f.read()

    sql_content = sql_content.replace("@source_database", database_source)
    sql_content = sql_content.replace("@target_database", database_target)

    print(f"[001.Import_vocabulary_copy] {database_source} → {database_target}")
    print("  Executing SQL (CONCEPT ~10M rows may take 2-5 min)...")

    with engine.connect() as conn:
        conn.execute(text(sql_content))
    print("[Done] Vocabulary copied.")
