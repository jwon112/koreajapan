"""
001.Import_voca.sql 실행 (CSV BULK INSERT)
- SQL: inst/sql/sql_server/ (한국 구조와 동일)
- Python: 파라미터 치환 후 실행만
"""

import os
import urllib

from sqlalchemy import create_engine, text

# ------------------------------------------------------
# 설정
# ------------------------------------------------------

server = r"DESKTOP-HBA9S76\SQLEXPRESS01"
database_target = "japan_cohort_cdm"
use_windows_auth = True

_script_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.normpath(os.path.join(_script_dir, "..", "..", ".."))
voca_folder = os.path.join(_project_root, "vocabulary")
sql_path = os.path.normpath(os.path.join(
    _script_dir, "..", "inst", "sql", "sql_server",
    "001.Import_voca.sql"
))

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

params = urllib.parse.quote_plus(conn_str)
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}", isolation_level="AUTOCOMMIT")

# ------------------------------------------------------
# SQL 로드 → 치환 → 실행
# ------------------------------------------------------

if __name__ == "__main__":
    if not os.path.exists(sql_path):
        raise FileNotFoundError(f"SQL not found: {sql_path}")
    if not os.path.isdir(voca_folder):
        raise FileNotFoundError(f"Vocabulary folder not found: {voca_folder}")

    required = ["CONCEPT.csv", "CONCEPT_SYNONYM.csv", "CONCEPT_RELATIONSHIP.csv", "CONCEPT_ANCESTOR.csv",
                "DRUG_STRENGTH.csv", "VOCABULARY.csv", "DOMAIN.csv", "CONCEPT_CLASS.csv", "RELATIONSHIP.csv"]
    for f in required:
        if not os.path.exists(os.path.join(voca_folder, f)):
            raise FileNotFoundError(f"Required file missing: {voca_folder}/{f}")

    with open(sql_path, "r", encoding="utf-8") as f:
        sql_content = f.read()

    sql_content = sql_content.replace("@Mapping_database", database_target)
    sql_content = sql_content.replace("@vocaFolder", voca_folder)

    print(f"[001.Import_voca] {voca_folder} → {database_target}")
    print("  Executing SQL (may take several minutes)...")

    with engine.connect() as conn:
        conn.execute(text(sql_content))
    print("[Done] Vocabulary imported.")
