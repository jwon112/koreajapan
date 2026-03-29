"""
일본 cohort SAS 파일 → SQL Server 적재 스크립트
- patient, claims, diagnosis, drug, procedure, *_master 파일을 DB 테이블로 로드
"""

from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
import os
import time
import pandas as pd
import pyreadstat
import urllib


def _format_elapsed(seconds):
    """초 → 'Xm Ys' 형식"""
    m = int(seconds // 60)
    s = int(seconds % 60)
    if m > 0:
        return f"{m}m {s}s"
    return f"{s}s"


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

# 이 크기(MB) 이상이면 청크 단위로 읽어서 적재 (메모리 절약)
CHUNK_THRESHOLD_MB = 300
SAS_CHUNK_ROWS = 200_000
SQL_CHUNK_ROWS = 50_000


def load_sas_to_sql(engine, schema, sas_path, table_name, idx, total):
    """단일 SAS 파일을 SQL Server 테이블로 로드 (대용량은 청크 방식)"""
    if not os.path.exists(sas_path):
        print(f"[{idx}/{total}] [SKIP] File not found: {os.path.basename(sas_path)}")
        return

    file_size_mb = os.path.getsize(sas_path) / (1024 * 1024)
    t0 = time.time()

    if file_size_mb >= CHUNK_THRESHOLD_MB:
        _load_sas_chunked(engine, schema, sas_path, table_name, idx, total, file_size_mb, t0)
    else:
        _load_sas_full(engine, schema, sas_path, table_name, idx, total, file_size_mb, t0)


def _load_sas_full(engine, schema, sas_path, table_name, idx, total, file_size_mb, t0):
    """소용량: 전체 읽기 후 to_sql (chunksize로 INSERT 배치)"""
    print(f"[{idx}/{total}] {table_name} ({file_size_mb:.1f} MB) - Reading SAS...")
    df, _ = pyreadstat.read_sas7bdat(sas_path)
    t_read = time.time() - t0
    print(f"         Read {len(df):,} rows in {_format_elapsed(t_read)}")

    print(f"         Writing to {schema}.{table_name} (chunksize={SQL_CHUNK_ROWS:,})...")
    t1 = time.time()
    df.to_sql(
        name=table_name,
        con=engine,
        schema=schema,
        if_exists="replace",
        index=False,
        chunksize=SQL_CHUNK_ROWS
    )
    t_write = time.time() - t1
    print(f"         Done in {_format_elapsed(t_write)} (total: {_format_elapsed(time.time() - t0)})")


def _load_sas_chunked(engine, schema, sas_path, table_name, idx, total, file_size_mb, t0):
    """대용량: SAS 청크 단위 읽기 → SQL 청크 단위 적재 (메모리 절약)"""
    print(f"[{idx}/{total}] {table_name} ({file_size_mb:.1f} MB) - Chunked load (SAS {SAS_CHUNK_ROWS:,} rows/chunk)...")
    row_offset = 0
    sas_chunk_idx = 0
    total_rows = 0
    first_chunk = True

    while True:
        df, _ = pyreadstat.read_sas7bdat(
            sas_path,
            row_offset=row_offset,
            row_limit=SAS_CHUNK_ROWS
        )
        if df.empty:
            break

        total_rows += len(df)
        t_read = time.time() - t0
        print(f"         SAS chunk #{sas_chunk_idx} | {len(df):,} rows | total {total_rows:,} | {_format_elapsed(t_read)}")

        if_exists = "replace" if first_chunk else "append"
        for start in range(0, len(df), SQL_CHUNK_ROWS):
            end = min(start + SQL_CHUNK_ROWS, len(df))
            sub = df.iloc[start:end]
            sub.to_sql(
                name=table_name,
                con=engine,
                schema=schema,
                if_exists=if_exists,
                index=False,
                chunksize=SQL_CHUNK_ROWS
            )
            if_exists = "append"

        first_chunk = False
        sas_chunk_idx += 1
        row_offset += SAS_CHUNK_ROWS

    print(f"         Done: {total_rows:,} rows in {_format_elapsed(time.time() - t0)}")


# ------------------------------------------------------
# 3) Main
# ------------------------------------------------------

if __name__ == "__main__":
    # workspace 루트 기준 데이터 경로 (scripts → etlJapanCohort → ETL---Japan-Cohort → koreajapan)
    _script_dir = os.path.dirname(os.path.abspath(__file__))
    _project_root = os.path.normpath(os.path.join(_script_dir, "..", "..", ".."))
    base_dir = os.path.join(_project_root, "cohort1.0", "japan", "japan_data")

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

    total_files = len(file_table_map)
    total_start = time.time()
    print(f"\n--- Loading {total_files} files into {database_raw} ---\n")

    for idx, (sas_filename, table_name) in enumerate(file_table_map, start=1):
        sas_path = os.path.join(base_dir, sas_filename)
        load_sas_to_sql(
            engine=engine_raw,
            schema=schema,
            sas_path=sas_path,
            table_name=table_name,
            idx=idx,
            total=total_files
        )

    total_elapsed = time.time() - total_start
    print(f"\n[ALL DONE] {total_files} files loaded in {_format_elapsed(total_elapsed)}")
