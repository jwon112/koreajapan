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
database_raw = "nhisnsc2013original"

# Windows 인증
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

# SQL SERVER 버전 출력
with engine_master.connect() as conn:
    print(conn.execute(text("SELECT @@VERSION")).fetchone())


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
# 2) SAS Loader Functions (경로 문제 100% 해결)
# ------------------------------------------------------

def collect_all_columns(base_dir, filename_pattern, years):

    # ⭐ filename_pattern 앞에 절대 슬래시 넣지 말 것
    #    join 자동 처리 방식 사용
    #    예: filename_pattern = "nhid_gj_{year}.sas7bdat"

    all_cols = set()

    for y in years:
        filename = filename_pattern.format(year=y)
        sas_path = os.path.join(base_dir, filename)

        print(f"[INFO] Scanning columns for year {y}: {sas_path}")

        if not os.path.exists(sas_path):
            print(f"[WARN] File not found, skipping: {sas_path}")
            continue

        _, meta = pyreadstat.read_sas7bdat(sas_path, metadataonly=True)
        all_cols.update(meta.column_names)

    all_cols = list(all_cols)
    print(f"[INFO] Total {len(all_cols)} columns found across years.")
    return all_cols



def load_sas_years_to_sql(
    engine,
    schema,
    table_name,
    base_dir,
    filename_pattern,   
    years,
    all_columns        
):
    first_year = True

    for y in years:
        filename = filename_pattern.format(year=y)
        sas_path = os.path.join(base_dir, filename)

        print(f"[INFO] Reading SAS file for year {y}: {sas_path}")

        if not os.path.exists(sas_path):
            print(f"[WARN] File not found, skipping: {sas_path}")
            continue

        df, meta = pyreadstat.read_sas7bdat(sas_path)
        print(f"[INFO] Loaded {len(df):,} rows for year {y}")

        # 채워넣기
        for col in all_columns:
            if col not in df.columns:
                df[col] = pd.NA

        df = df[all_columns]

        if_exists_mode = "replace" if first_year else "append"

        print(f"[INFO] Writing to SQL table {schema}.{table_name} (if_exists='{if_exists_mode}')")

        df.to_sql(
            name=table_name,
            con=engine,
            schema=schema,
            if_exists=if_exists_mode,
            index=False
        )

        first_year = False
        print(f"[INFO] Finished writing year {y}")

    print(f"[DONE] All available years loaded into {schema}.{table_name}")




# ------------------------------------------------------
# 3) Main
# ------------------------------------------------------

if __name__ == "__main__":

    # ⭐ 경로는 이렇게 지정
    base_dir = r"C:\Users\chaeyoon\Desktop\koreajapan\cohort1.0"



    schema = "dbo"
    years = range(2002, 2003)

    # 파일 패턴 — 
    filename_pattern = "nhid_gj_{year}.sas7bdat"


    # Step 1: Collect Columns
    all_cols_gj = collect_all_columns(base_dir, filename_pattern, years)

    # Step 2: Load
    load_sas_years_to_sql(
        engine           = engine_raw,
        schema           = schema,
        table_name       = "NHID_GJ",
        base_dir         = base_dir,
        filename_pattern = filename_pattern,
        years            = years,
        all_columns      = all_cols_gj
    )
