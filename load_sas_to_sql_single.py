import os
import pandas as pd
import pyreadstat
from sqlalchemy import create_engine, text
import urllib


# -----------------------------------------
# 1) SQL Server 연결
# -----------------------------------------
server = r"DESKTOP-HBA9S76\SQLEXPRESS01"
database = "nhisnsc2013original"
table_name = "NHID_GJ_SINGLE"
schema = "dbo"

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


# -----------------------------------------
# 2) SAS 파일 하나만 업로드하는 함수
# -----------------------------------------
def upload_single_sas_file(sas_path, table_name, schema="dbo", chunk_size=100):
    print(f"[INFO] Loading SAS file: {sas_path}")

    if not os.path.exists(sas_path):
        print(f"[ERROR] File does not exist: {sas_path}")
        return

    # 전체 SAS 로딩
    df, meta = pyreadstat.read_sas7bdat(sas_path)
    print(f"[INFO] Loaded total {len(df):,} rows.")

    # SQL 테이블 생성 (replace)
    print(f"[INFO] Creating table {schema}.{table_name} ...")
    df.head(0).to_sql(table_name, engine, schema=schema, if_exists="replace", index=False)

    # chunk 업로드
    total_rows = len(df)
    print(f"[INFO] Uploading to SQL in chunks of {chunk_size} rows...")

    for start in range(0, total_rows, chunk_size):
        end = min(start + chunk_size, total_rows)
        chunk = df.iloc[start:end]

        chunk.to_sql(
            table_name,
            engine,
            schema=schema,
            if_exists="append",
            index=False
        )

        # 진행률 로그 출력
        print(f"[UPLOAD] {end:,}/{total_rows:,} rows uploaded ({(end/total_rows)*100:.1f}%)")

    print(f"[DONE] File upload complete: {schema}.{table_name}")


# -----------------------------------------
# 3) 실행
# -----------------------------------------
if __name__ == "__main__":
    sas_path = r"C:\Users\chaeyoon\Desktop\koreajapan\cohort1.0\yk_2002_2013.sas7bdat"

    upload_single_sas_file(
        sas_path=sas_path,
        table_name="NHID_YK",
        schema="dbo",
        chunk_size=100      
    )
