import os
import pandas as pd
import pyreadstat
from sqlalchemy import create_engine, text
import urllib


# --------------------------------------------------
# 1) SQL Server 연결 설정
# --------------------------------------------------
server = r"DESKTOP-HBA9S76\SQLEXPRESS01"
database = "nhisnsc2013original"
schema = "dbo"
table_name = "NHID_30"    # 기존 테이블 유지

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
# 2) 컬럼 합집합 스캔
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
            print(f"[ERROR] Failed scanning year {y}: {e}")
            continue

    all_cols = list(all_cols)
    print(f"[INFO] Total {len(all_cols)} columns found across years.")
    return all_cols


# --------------------------------------------------
# 3) 연도별 파일 업로드 (100개 단위 로그 + 오류 무시)
# --------------------------------------------------
def upload_year_file(sas_path, engine, schema, table_name, all_columns, first_year=False, chunk_size=100):

    print(f"[INFO] Loading: {sas_path}")

    if not os.path.exists(sas_path):
        print(f"[WARN] Not found: {sas_path}")
        return

    try:
        df, meta = pyreadstat.read_sas7bdat(sas_path)
    except Exception as e:
        print(f"[ERROR] Could not read SAS file: {e}")
        return

    total_rows = len(df)
    print(f"[INFO] Loaded {total_rows:,} rows")

    for col in all_columns:
        if col not in df.columns:
            df[col] = pd.NA

    df = df[all_columns]

    # 첫 번째 업로드만 테이블 생성
    if first_year:
        print(f"[INFO] Creating table {schema}.{table_name} (replace)")
        df.head(0).to_sql(table_name, engine, schema=schema, if_exists="replace", index=False)

    print(f"[INFO] Uploading in chunks of {chunk_size}")
    for start in range(0, total_rows, chunk_size):
        end = min(start + chunk_size, total_rows)
        chunk = df.iloc[start:end]

        chunk.to_sql(table_name, engine, schema=schema, if_exists="append", index=False)

        print(f"[UPLOAD] {end:,}/{total_rows:,} rows uploaded ({end/total_rows*100:.1f}%)")

    print(f"[DONE] Finished year file: {sas_path}")


# --------------------------------------------------
# 4) Main Pipeline
# --------------------------------------------------
if __name__ == "__main__":

    base_dir = r"C:\Users\chaeyoon\Desktop\koreajapan\cohort1.0"
    filename_pattern = "nhid_gy30_t1_{year}.sas7bdat"
    years = range(2006, 2010)

    # Step 1: 컬럼 스캔
    all_columns = collect_union_columns(base_dir, filename_pattern, years)

    # ---------------------
    # ✔ 이미 2004년까지 들어간 상태라면
    #    2006년부터 새로 append
    # ---------------------
    START_YEAR = 2006     # 원하는 시작 연도
    first_year_flag = False   # 테이블 recreate 하지 않음

    for y in years:
        if y < START_YEAR:
            print(f"[SKIP] {y} already inserted. Skipping...")
            continue

        sas_path = os.path.join(base_dir, filename_pattern.format(year=y))

        try:
            upload_year_file(
                sas_path=sas_path,
                engine=engine,
                schema=schema,
                table_name=table_name,
                all_columns=all_columns,
                first_year=first_year_flag,
                chunk_size=100
            )
            first_year_flag = False  # always append now
        except Exception as e:
            print(f"[ERROR] Upload failed for {y}: {e}")
            print("[INFO] Continuing to next year...")
            continue

    print("[ALL DONE] Processing complete!")
