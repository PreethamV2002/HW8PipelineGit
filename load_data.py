# load_data.py
# Loads two CSVs into Azure SQL. Creates tables if missing, widens columns,
# truncates for idempotent re-runs, then bulk-inserts and prints row counts.

import os
import pandas as pd
import sqlalchemy as sa

# ---------- Config provided by the pipeline (env vars) ----------
SERVER   = os.environ["AZ_SQLSERVER"]      # e.g., preethampreethamhw8server.database.windows.net
DATABASE = os.environ["AZ_DBNAME"]         # e.g., HW8PipelineDB
UID      = os.environ["AZ_SQLUSER"]        # e.g., preetham
PWD      = os.environ["AZ_SQLPASSWORD"]

# ---------- File paths (repo-relative) ----------
DATA_DIR   = os.path.join(os.getcwd(), "data")
DAILY_CSV  = os.path.join(DATA_DIR, "2021-01-19--data_01be88c2-0306-48b3-0042-fa0703282ad6_1304_5_0.csv")
BRAND_CSV  = os.path.join(DATA_DIR, "brand-detail-url-etc_0_0_0.csv")

# ---------- SQLAlchemy engine (ODBC Driver 18) ----------
odbc = (
    "Driver=ODBC Driver 18 for SQL Server;"
    f"Server=tcp:{SERVER},1433;"
    f"Database={DATABASE};"
    f"Uid={UID};"
    f"Pwd={PWD};"
    "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
)
engine = sa.create_engine(
    f"mssql+pyodbc:///?odbc_connect={sa.engine.url.quote_plus(odbc)}",
    fast_executemany=True,
)

# ---------- Create tables if they don't exist ----------
DDL_CREATE = """
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name='ConsumerSpendDaily' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
  CREATE TABLE dbo.ConsumerSpendDaily(
    BRAND_ID      INT             NULL,
    BRAND_NAME    NVARCHAR(400)   NULL,
    SPEND_AMOUNT  DECIMAL(18,4)   NULL,
    STATE_ABBR    NVARCHAR(50)    NULL,
    TRANS_COUNT   DECIMAL(18,4)   NULL,
    TRANS_DATE    DATETIME2       NULL,
    VERSION       DATETIME2       NULL
  );
END;

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name='BrandDetail' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
  CREATE TABLE dbo.BrandDetail(
    BRAND_ID           INT              NULL,
    BRAND_NAME         NVARCHAR(400)    NULL,
    BRAND_TYPE         NVARCHAR(100)    NULL,
    BRAND_URL_ADDR     NVARCHAR(MAX)    NULL,
    INDUSTRY_NAME      NVARCHAR(200)    NULL,
    SUBINDUSTRY_ID     INT              NULL,
    SUBINDUSTRY_NAME   NVARCHAR(200)    NULL
  );
END;
"""
with engine.begin() as conn:
    conn.exec_driver_sql(DDL_CREATE)

# ---------- Widen columns (always safe; idempotent) ----------
DDL_WIDEN = """
ALTER TABLE dbo.BrandDetail       ALTER COLUMN BRAND_NAME         NVARCHAR(400)    NULL;
ALTER TABLE dbo.BrandDetail       ALTER COLUMN BRAND_TYPE         NVARCHAR(100)    NULL;
ALTER TABLE dbo.BrandDetail       ALTER COLUMN BRAND_URL_ADDR     NVARCHAR(MAX)    NULL;
ALTER TABLE dbo.BrandDetail       ALTER COLUMN INDUSTRY_NAME      NVARCHAR(200)    NULL;
ALTER TABLE dbo.BrandDetail       ALTER COLUMN SUBINDUSTRY_NAME   NVARCHAR(200)    NULL;

ALTER TABLE dbo.ConsumerSpendDaily ALTER COLUMN BRAND_NAME        NVARCHAR(400)    NULL;
ALTER TABLE dbo.ConsumerSpendDaily ALTER COLUMN STATE_ABBR        NVARCHAR(50)     NULL;
"""
with engine.begin() as conn:
    conn.exec_driver_sql(DDL_WIDEN)

# ---------- Read CSVs ----------
brand_df = pd.read_csv(BRAND_CSV, encoding="utf-8")
daily_df = pd.read_csv(DAILY_CSV, encoding="utf-8")

# Normalize column names
brand_df.columns = [c.strip() for c in brand_df.columns]
daily_df.columns = [c.strip() for c in daily_df.columns]

# Coerce dates if present
for col in ("TRANS_DATE", "VERSION"):
    if col in daily_df.columns:
        daily_df[col] = pd.to_datetime(daily_df[col], errors="coerce")

# ---------- Idempotent loads: truncate then insert ----------
with engine.begin() as conn:
    conn.exec_driver_sql("TRUNCATE TABLE dbo.BrandDetail;")
    conn.exec_driver_sql("TRUNCATE TABLE dbo.ConsumerSpendDaily;")

# Bulk insert
brand_df.to_sql("BrandDetail", con=engine, schema="dbo", if_exists="append", index=False, chunksize=5000)
daily_df.to_sql("ConsumerSpendDaily", con=engine, schema="dbo", if_exists="append", index=False, chunksize=5000)

# ---------- Smoke checks ----------
with engine.begin() as conn:
    print("Counts:")
    rows = conn.exec_driver_sql("SELECT 'BrandDetail' AS TableName, COUNT(*) AS Rows FROM dbo.BrandDetail").fetchall()
    print(rows)
    rows = conn.exec_driver_sql("SELECT 'ConsumerSpendDaily' AS TableName, COUNT(*) AS Rows FROM dbo.ConsumerSpendDaily").fetchall()
    print(rows)
    rows = conn.exec_driver_sql("""
        SELECT TOP 5 STATE_ABBR, SUM(SPEND_AMOUNT) AS TotalSpend
        FROM dbo.ConsumerSpendDaily
        GROUP BY STATE_ABBR
        ORDER BY TotalSpend DESC
    """).fetchall()
    print("Top states by spend (top 5):", rows)

print("Load complete ✅")
