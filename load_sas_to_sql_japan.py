"""
일본 cohort SAS 파일 → SQL Server 적재 스크립트
- patient, claims, diagnosis, drug, procedure, *_master 파일을 DB 테이블로 로드
"""

from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
import os
import pandas as pd
import pyreadstat
import urllib


# ------------------------------------------------------
# 1) SQL Server Connection 설정
# ------------------------------------------------------

server = r"DESKTOP-HBA9S76\SQLEXPRESS01"
database_master = "master"
database_raw = "japan_cohort_raw"

use_windows_auth = True


# ------------------------------------------------------
# 1-A) MASTER DB ENGINE
# ------------------------------------------------------

if use_windows_auth:
    connection_string_master = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database_master};"
        f"Trusted_Connection=Yes;"
        f"Encrypt=no;"
    )
else:
    username = "sa"
    password_plain = "KoreaJapan44@"
    connection_string_master = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database_master};"
        f"UID={username};"
        f"PWD={password_plain};"
        f"Encrypt=no;"
    )

params_master = urllib.parse.quote_plus(connection_string_master)
engine_master = create_engine(
    f"mssql+pyodbc:///?odbc_connect={params_master}",
    isolation_level="AUTOCOMMIT"
)


# ------------------------------------------------------
# 1-B) RAW DATABASE 생성
# ------------------------------------------------------

with engine_master.connect() as conn:
    result = conn.execute(text(f"SELECT DB_ID('{database_raw}')"))
    if result.scalar() is None:
        print(f"Database '{database_raw}' does not exist. Creating...")
        conn.execute(text(f"CREATE DATABASE {database_raw}"))
    else:
        print(f"Database '{database_raw}' already exists.")


# ------------------------------------------------------
# 1-C) RAW DB ENGINE
# ------------------------------------------------------

if use_windows_auth:
    connection_string_raw = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database_raw};"
        f"Trusted_Connection=Yes;"
        f"Encrypt=no;"
    )
else:
    connection_string_raw = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database_raw};"
        f"UID={username};"
        f"PWD={password_plain};"
        f"Encrypt=no;"
    )

params_raw = urllib.parse.quote_plus(connection_string_raw)
engine_raw = create_engine(f"mssql+pyodbc:///?odbc_connect={params_raw}")
print("Connected to RAW database successfully.")


# ------------------------------------------------------
# 2) SAS Loader
# ------------------------------------------------------

def load_sas_to_sql(engine, schema, sas_path, table_name):
    """단일 SAS 파일을 SQL Server 테이블로 로드"""
    if not os.path.exists(sas_path):
        print(f"[WARN] File not found, skipping: {sas_path}")
        return

    print(f"[INFO] Loading: {sas_path}")
    df, meta = pyreadstat.read_sas7bdat(sas_path)
    print(f"[INFO] Loaded {len(df):,} rows")

    print(f"[INFO] Writing to {schema}.{table_name}")
    df.to_sql(
        name=table_name,
        con=engine,
        schema=schema,
        if_exists="replace",
        index=False
    )
    print(f"[DONE] {table_name} loaded.")


# ------------------------------------------------------
# 3) Main
# ------------------------------------------------------

if __name__ == "__main__":

    base_dir = r"C:\Users\chaeyoon\Desktop\koreajapan\cohort1.0\japan\japan_data"
    schema = "dbo"

    # (SAS 파일명, DB 테이블명) 매핑
    file_table_map = [
        ("patient_50.sas7bdat", "JP_PATIENT"),
        ("claims_50.sas7bdat", "JP_CLAIMS"),
        ("diagnosis_50.sas7bdat", "JP_DIAGNOSIS"),
        ("drug_50.sas7bdat", "JP_DRUG"),
        ("procedure_50.sas7bdat", "JP_PROCEDURE"),
        ("diagnosis_master.sas7bdat", "JP_DIAGNOSIS_MASTER"),
        ("drug_master.sas7bdat", "JP_DRUG_MASTER"),
        ("procedure_master.sas7bdat", "JP_PROCEDURE_MASTER"),
    ]

    for sas_filename, table_name in file_table_map:
        sas_path = os.path.join(base_dir, sas_filename)
        load_sas_to_sql(
            engine=engine_raw,
            schema=schema,
            sas_path=sas_path,
            table_name=table_name
        )

    print("\n[ALL DONE] Japan cohort data loaded into SQL Server.")
