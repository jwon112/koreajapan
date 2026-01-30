import os
import pandas as pd
import pyreadstat
from sqlalchemy import create_engine
import urllib


# --------------------------------------------------
# 1) SQL Server 연결 설정
# --------------------------------------------------
server = r"DESKTOP-HBA9S76\SQLEXPRESS01"
database = "nhisnsc2013original"
schema = "dbo"
table_name = "NHID_20"

connection_string = (
    f"DRIVER={{ODBC Driver 18 for SQL Server}};"
    f"SERVER={server};"
    f"DATABASE={database};"
    f"Trusted_Connection=Yes;"
    f"Encrypt=no;"
)

params = urllib.parse.quote_plus(connection_string)
engine = create_engine(
    f"mssql+pyodbc:///?odbc_connect={params}",
    isolation_level="AUTOCOMMIT"
)


# --------------------------------------------------
# 2) 모든 연도 파일 스캔 → 컬럼 합집합 생성
# --------------------------------------------------
def collect_union_columns(base_dir, filename_pattern, years):
    all_cols = set()

    for y in years:
        filename = filename_pattern.format(year=y)
        sas_path = os.path.join(base_dir, filename)

        print(f"[SCAN] {sas_path}")

        if not os.path.exists(sas_path):
            print(f"[WARN] File not found, skipping: {sas_path}")
            continue

        try:
            _, meta = pyreadstat.read_sas7bdat(sas_path, metadataonly=True)
            all_cols.update(meta.column_names)
        except Exception as e:
            print(f"[ERROR] Could not read metadata: {sas_path} | {e}")
            continue

    all_cols = list(all_cols)
    print(f"[INFO] Total {len(all_cols)} columns across all years.")
    return all_cols


# --------------------------------------------------
# 3) Chunk 방식 연도 파일 업로드
# --------------------------------------------------
def upload_year_file(sas_path, engine, schema, table_name, all_columns, first_year=False,
                     sas_chunk_size=200000, sql_chunk_size=50000):

    print(f"[INFO] Loading file by chunks: {sas_path}")

    if not os.path.exists(sas_path):
        print(f"[WARN] File not found: {sas_path}")
        return

    row_offset = 0
    chunk_idx = 0
    created_table = False

    while True:
        # SAS 파일 chunk 로딩
        df, meta = pyreadstat.read_sas7bdat(
            sas_path,
            row_offset=row_offset,
            row_limit=sas_chunk_size
        )

        if df.empty:   # 파일의 끝
            break

        print(f"[INFO] SAS chunk #{chunk_idx} loaded | rows={len(df):,}")

        # 누락 컬럼 추가
        for col in all_columns:
            if col not in df.columns:
                df[col] = pd.NA

        df = df[all_columns]

        # 첫 연도 & 첫 chunk → 테이블 생성
        if first_year and chunk_idx == 0:
            print(f"[INFO] Creating SQL table {schema}.{table_name}")
            df.head(0).to_sql(
                name=table_name,
                con=engine,
                schema=schema,
                if_exists="replace",
                index=False
            )
            created_table = True

        # SQL chunk 업로드
        for start in range(0, len(df), sql_chunk_size):
            end = min(start + sql_chunk_size, len(df))
            sub_chunk = df.iloc[start:end]

            sub_chunk.to_sql(
                name=table_name,
                con=engine,
                schema=schema,
                if_exists="append",
                index=False
            )
            print(f"[UPLOAD] SQL rows: {start:,}-{end:,} uploaded")

        chunk_idx += 1
        row_offset += sas_chunk_size

    print(f"[DONE] Finished file: {sas_path}")


# --------------------------------------------------
# 4) Main Pipeline
# --------------------------------------------------
if __name__ == "__main__":

    base_dir = r"C:\Users\chaeyoon\Desktop\koreajapan\cohort1.0"
    filename_pattern = "nhid_gy20_t1_{year}.sas7bdat"

    # 전체 스캔 연도
    scan_years = range(2002, 2014)

    # 업로드 연도
    upload_years = range(2010, 2011)

    # Step 1: 전체 컬럼 스캔
    all_columns = collect_union_columns(base_dir, filename_pattern, scan_years)

    # Step 2: 업로드 (2006~2009)
    first_year_flag = True

    for y in upload_years:
        sas_path = os.path.join(base_dir, filename_pattern.format(year=y))
        upload_year_file(
            sas_path=sas_path,
            engine=engine,
            schema=schema,
            table_name=table_name,
            all_columns=all_columns,
            first_year=first_year_flag,
            sas_chunk_size=200000,   # SAS 파일 로딩 chunk
            sql_chunk_size=50000     # SQL 업로드 chunk
        )
        first_year_flag = False

    print("[ALL DONE] Selected years successfully uploaded into SQL Server!")
