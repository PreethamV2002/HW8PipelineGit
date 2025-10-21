import os
import pandas as pd
import sqlalchemy as sa

# -------- Config from env (set by pipeline) ----------
SERVER   = os.environ["AZ_SQLSERVER"]              # e.g., preethampreethamhw8server.database.windows.net
DATABASE = os.environ["AZ_DBNAME"]                 # e.g., HW8PipelineDB
UID      = os.environ["AZ_SQLUSER"]                # e.g., preetham
PWD      = os.environ["AZ_SQLPASSWORD"]

# -------- File paths (repo-relative) -----------------
DATA_DIR = os.path.join(os.getcwd(), "data")
DAILY_CSV = os.path.join(DATA_DIR, "2021-01-19--data_01be88c2-0306-48b3-0042-fa0703282ad6_1304_5_0.csv")
BRAND_CSV = os.path.join(DATA_DIR, "brand-detail-url-etc_0_0_0.csv")

# -------- SQLAlchemy engine (ODBC Driver 18) --------
# URL-encoding for driver string
params = (
    "Driver=ODBC Driver 18 for SQL Server;"
    f"Server=tcp:{SERVER},1433;"
    f"Database={DATABASE};"
    f"Uid={UID};"
    f"Pwd={PWD};"
    "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
)
# Use "odbc_connect" to pass full ODBC string
engine = sa.create_engine(f"mssql+pyodbc:///?odbc_connect={sa.engine.url.quote_plus(params)}",
                          fast_executemany=True)

# -------- DDL: create tables if not exist -----------
DDL = """
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name='ConsumerSpendDaily')
BEGIN
  CREATE TABLE dbo.ConsumerSpendDaily(
    BRAND_ID INT NULL,
    BRAND_NAME NVARCHAR(100) NULL,
    SPEND_AMOUNT DECIMAL(18,4) NULL,
    STATE_ABBR NVARCHAR(100) NULL,
    TRANS_COUNT DECIMAL(18,4) NULL,
    TRANS_DATE DATETIME2 NULL,
    VERSION DATETIME2 NULL
  );
END;

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name='BrandDetail')
BEGIN
  CREATE TABLE dbo.BrandDetail(
    BRAND_ID INT NULL,
    BRAND_NAME NVARCHAR(100) NULL,
    BRAND_TYPE NVARCHAR(100) NULL,
    BRAND_URL_ADDR NVARCHAR(100) NULL,
    INDUSTRY_NAME NVARCHAR(100) NULL,
    SUBINDUSTRY_ID INT NULL,
    SUBINDUSTRY_NAME NVARCHAR(100) NULL
  );
END;
"""

with engine.begin() as conn:
    conn.exec_driver_sql(DDL)

# -------- Load CSVs with pandas ----------------------
brand_df = pd.read_csv(BRAND_CSV)
daily_df = pd.read_csv(DAILY_CSV)

# Optional: strip column names / normalize types
brand_df.columns = [c.strip() for c in brand_df.columns]
daily_df.columns = [c.strip() for c in daily_df.columns]

# Upsert strategy here is "replace all" for demo purposes:
with engine.begin() as conn:
    # empty tables first (if you want idempotent loads)
    conn.exec_driver_sql("TRUNCATE TABLE dbo.BrandDetail;")
    conn.exec_driver_sql("TRUNCATE TABLE dbo.ConsumerSpendDaily;")

# Bulk insert
brand_df.to_sql("BrandDetail", con=engine, schema="dbo", if_exists="append", index=False)
daily_df.to_sql("ConsumerSpendDaily", con=engine, schema="dbo", if_exists="append", index=False)

# -------- Smoke test output -------------------------
with engine.begin() as conn:
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
        res = conn.exec_driver_sql(sql)
        rows = res.fetchall()
        print(sql.strip().split('\n')[0], "->", rows)