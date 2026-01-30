from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

# DB Connection Information
server = "localhost"
port = 1433
username = "sa"
password_plain = "KoreaJapan44@"
password = quote_plus(password_plain)

engine = create_engine(
    f"mssql+pymssql://{username}:{password}@{server}:{port}/nhisnsc2013original"
)

with engine.connect() as conn:
    print("[INFO] Connected to nhisnsc2013original")

    # 1) Current Table list in Database
    result = conn.execute(text("SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES ORDER BY TABLE_SCHEMA, TABLE_NAME"))
    tables = result.fetchall()

    print("\n[INFO] Tables in nhisnsc2013original:")
    for schema, name in tables:
        print(f"  {schema}.{name}")

    # 2) NHID_GJ Structure
    print("\n[INFO] Columns of dbo.NHID_GJ:")
    result = conn.execute(text("""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = 'NHID_GJ'
        ORDER BY ORDINAL_POSITION
    """))
    cols = result.fetchall()
    for col, dtype in cols:
        print(f"  {col} ({dtype})")

    # 3) NHID_GJ Row Count
    result = conn.execute(text("SELECT COUNT(*) AS cnt FROM dbo.NHID_GJ"))
    row_count = result.scalar()
    print(f"\n[INFO] Row count of dbo.NHID_GJ: {row_count:,}")

    # 4) NHID_GJ Sample 
    print("\n[INFO] Sample 5 rows from dbo.NHID_GJ:")
    result = conn.execute(text("SELECT TOP 5 * FROM dbo.NHID_GJ"))
    rows = result.fetchall()

    # Column Name
    colnames = result.keys()
    print("  | " + " | ".join(colnames) + " |")
    for r in rows:
        print("  | " + " | ".join(str(v) if v is not None else "NULL" for v in r) + " |")