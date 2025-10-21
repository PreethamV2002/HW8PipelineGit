# load_data.py
# Ingests two CSVs into Azure SQL with safe column sizes and idempotent loads.

import os
import pandas as pd
import sqlalchemy as sa

# ---------- Config (injected by the pipeline as env vars) ----------
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

# ---------- DDL: create tables if missing ----------
DDL = """
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
    conn.exec_driver_sql(DDL)

# ---------- WIDEN columns defensively (prevents truncation errors) ----------
# If tables already exist with smaller sizes, widen them.
WIDEN = """
IF COL_LENGTH('dbo.BrandDetail','BRAND_NAME')       < 400 ALTER TABLE dbo.BrandDetail ALTER COLUMN BRAND_NAME NVARCHAR(400) NULL;
IF COL_LENGTH('dbo.BrandDetail','BRAND_TYPE')       < 100 ALTER TABLE dbo.BrandDetail ALTER COLUMN BRAND_TYPE NVARCHAR(100) NULL;
IF COL_LENGTH('dbo.BrandDetail','BRAND_URL_ADDR')   IS NOT NULL AND
   (SELECT MAX(D.character_maximum_length) FROM INFORMATION_SCHEMA.COLUMNS D
     WHERE D.TABLE_SCHEMA='dbo' AND D.TABLE_NAME='BrandDetail' AND D.COLUMN_NAME='BRAND_URL_ADDR') < -1
BEGIN
    -- no-op (already MAX)
END
ELSE
    ALTER TABLE dbo.BrandDetail ALTER COLUMN BRAND_URL_ADDR NVARCHAR(MAX) NULL;

IF COL_LENGTH('dbo.BrandDetail','INDUSTRY_NAME')    < 200 ALTER TABLE dbo.BrandDetail ALTER COLUMN INDUSTRY_NAME NVARCHAR(200) NULL;
IF COL_LENGTH('dbo.BrandDetail','SUBINDUSTRY_NAME') < 200 ALTER TABLE dbo.BrandDetail ALTER COLUMN SUBINDUSTRY_NAME NVARCHAR(200) NULL;

IF COL_LENGTH('dbo.ConsumerSpendDaily','BRAND_NAME') < 400 ALTER TABLE dbo.ConsumerSpendDaily ALTER COLUMN BRAND_NAME NVARCHAR(400) NULL;
IF COL_LENGTH('dbo.ConsumerSpendDaily','STATE_ABBR') < 50  ALTER TABLE dbo.ConsumerSpendDaily ALTER COLUMN STATE_ABBR NVARCHAR(50)  NULL;
"""
with engine.begin() as conn:
    conn.exec_driver_sql(WIDEN)

# ---------- Load CSVs with pandas ----------
# Read robustly (UTF-8), leave blanks as NaN (good for NULL)
brand_df = pd.read_csv(BRAND_CSV, encoding="utf-8")
daily_df = pd.read_csv(DAILY_CSV, encoding="utf-8")

# Normalize column names (strip spaces)
brand_df.columns = [c.strip() for c in brand_df.columns]
daily_df.columns = [c.strip() for c in daily_df.columns]

# Optional: coerce dates if present as strings
for col in ("TRANS_DATE", "VERSION"):
    if col in daily_df.columns:
        daily_df[col] = pd.to_datetime(daily_df[col], errors="coerce")

# ---------- Make loads idempotent ----------
with engine.begin() as conn:
    conn.exec_driver_sql("TRUNCATE TABLE dbo.BrandDetail;")
    conn.exec_driver_sql("TRUNCATE TABLE dbo.ConsumerSpendDaily;")

# ---------- Bulk insert ----------
brand_df.to_sql("BrandDetail", con=engine, schema="dbo", if_exists="append", index=False, chunksize=5000)
daily_df.to_sql("ConsumerSpendDaily", con=engine, schema="dbo", if_exists="append", index=False, chunksize=5000)

# ---------- Smoke tests ----------
with engine.begin() as conn:
    print("Counts:")
    for sql in [
        "SELECT 'BrandDetail' AS TableName, COUNT(*) AS Rows FROM dbo.BrandDetail",
        "SELECT 'ConsumerSpendDaily' AS TableName, COUNT(*) AS Rows FROM dbo.ConsumerSpendDaily",
        """
        SELECT TOP 5 STATE_ABBR, SUM(SPEND_AMOUNT) AS TotalSpend
        FROM dbo.ConsumerSpendDaily
        GROUP BY STATE_ABBR
        ORDER BY TotalSpend DESC
        """
    ]:
        rows = conn.exec_driver_sql(sql).fetchall()
        print(sql.splitlines()[0].strip(), "->", rows)

print("Load complete ✅")
