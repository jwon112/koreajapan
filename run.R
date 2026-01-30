# ------------------------------------------------------------
# 0) 로컬 ETL 함수 로드
# ------------------------------------------------------------

source("C:/Users/chaeyoon/Desktop/koreajapan/ETL---Korean-NSC/etlKoreanNSC/R/executeETL.R")


# ------------------------------------------------------------
# 1) SQL Server 연결 (odbc)
# ------------------------------------------------------------

library(odbc)
library(DBI)

con <- dbConnect(
    odbc(),
    Driver = "ODBC Driver 17 for SQL Server",
    Server = "DESKTOP-HBA9S76\\SQLEXPRESS01",
    Database = "nhisnsc2013original",
    Trusted_Connection = "Yes",
    Encrypt = "no"
)


# ------------------------------------------------------------
# 2) Output 폴더 설정
# ------------------------------------------------------------

outputFolder <- "C:/Users/chaeyoon/Desktop/Lab/korea_japan/output"
if (!dir.exists(outputFolder)) dir.create(outputFolder, recursive = TRUE)


# ------------------------------------------------------------
# 3) ETL 실행
# ------------------------------------------------------------

executeNHISETL(
    
    # --- RAW DB + 스키마 ---
    NHISNSC_rawdata   = "nhisnsc2013original.dbo",
    
    # --- CDM DB + 스키마 ---
    NHISNSC_database  = "nhisnsc2013cdm.dbo",
    
    # --- Mapping DB ---
    Mapping_database  = "nhisnsc2013mapping.dbo",
    
    # --- RAW 테이블 ---
    NHIS_JK  = "NHID_JK",
    NHIS_20T = "NHID_20",
    NHIS_30T = 'NHID_30',
    NHIS_40T = "NHID_40",
    NHIS_60T = "NHID_50",
    NHIS_GJ  = "NHID_GJ",
    NHIS_YK  = "NHID_YK",
    
    # --- 연결 + 로그 출력 ---
    connection   = con,
    outputFolder = outputFolder,
    
    # ------------------------------------------------------------
    # 선택한 ETL 단계
    # ------------------------------------------------------------
    
    CDM_ddl            = TRUE,     # CDM 테이블 생성 여부
    master_table       = FALSE,
    location           = TRUE,
    care_site          = TRUE,      # <-- 실행할 단계
    person             =TRUE,
    observation_period = TRUE,      # <-- 실행할 단계
    condition_occurrence = TRUE,
    payer_plan_period  = TRUE,
    cost               = TRUE,
    generateEra        = TRUE,
    dose_era           = TRUE,
    cdm_source         = TRUE,
    indexing           = TRUE,
    constraints        = TRUE,
    data_cleansing     = TRUE
)

