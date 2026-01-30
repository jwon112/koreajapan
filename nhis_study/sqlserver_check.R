# Library
library(haven)
library(DatabaseConnector)
library(SqlRender)
library(dplyr)
# --- Setting ---
my_user <- "sa"
my_password <- "KoreaJapan44@" 
pathToDriver <- "~/jdbcDrivers_sqlserver"

# Connection Detail
connectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = "sql server",
  server = "localhost", 
  port = 1433,
  user = my_user,
  password = my_password,
  pathToDriver = pathToDriver
)

# Connection trial
connection <- DatabaseConnector::connect(connectionDetails)
#print("🎉 SQL Server Connected")
D#atabaseConnector::disconnect(connection)

DatabaseConnector::querySql(connection, "SELECT name FROM sys.databases;")

tables <- DatabaseConnector::querySql(
  connection,
  "
  SELECT TABLE_NAME
  FROM nhisnsc2013original.INFORMATION_SCHEMA.TABLES
  WHERE TABLE_SCHEMA = 'dbo'
  ORDER BY TABLE_NAME;
  "
)
tables

gj_cols <- DatabaseConnector::querySql(
  connection,
  "
  SELECT 
    COLUMN_NAME,
    DATA_TYPE,
    CHARACTER_MAXIMUM_LENGTH,
    IS_NULLABLE
  FROM nhisnsc2013original.INFORMATION_SCHEMA.COLUMNS
  WHERE TABLE_SCHEMA = 'dbo'
    AND TABLE_NAME   = 'NHID_GJ'
  ORDER BY ORDINAL_POSITION;
  "
)

gj_cols

gj_sample <- DatabaseConnector::querySql(
  connection,
  "
  SELECT TOP 100 *
  FROM nhisnsc2013original.dbo.NHID_GJ;
  "
)

str(gj_sample)


library(dplyr)
glimpse(gj_sample)

DatabaseConnector::disconnect(connection)