# 1. 라이브러리 로드
library(DatabaseConnector)
library(SqlRender)
library(etlKoreanNSC) # 패키지가 로드되어 있어야 합니다 (devtools::load_all() 사용 권장)

# 2. JDBC 드라이버 경로 (아까 설정한 경로)
driverPath <- "C:/Program Files/sqljdbc_13.2/kor/jars" 

# 3. 연결 정보 설정 (connectionDetails - 전역 변수로 생성)
# 주의: 함수 내부에서 이 변수 이름을 그대로 참조하므로 이름을 바꾸지 마세요.
connectionDetails <- createConnectionDetails(
  dbms = "sql server",
  server = "localhost;encrypt=true;trustServerCertificate=true",      # 또는 "localhost\\SQLEXPRESS"
  user = "jaewon",               # 본인 ID
  password = "00tocjw112",# 본인 PW
  pathToDriver = driverPath
)

# 4. 실제 DB 연결 생성 (함수에 전달할 객체)
conn <- connect(connectionDetails)

# 5. 테이블 이름 매핑
# MSSQL에 복원된 실제 테이블 이름과 변수를 매칭합니다.
# 예: 실제 테이블이 dbo.JK_T 라면 "JK_T"라고 적어야 합니다.
nhis_tables <- list(
  JK = "NHID_JK",   # 자격(Jakyuk) 테이블명
  T20 = "NHID_20", # 20 테이블명 (명세서 일반)
  T30 = "NHID_30", # 30 테이블명 (진료내역)
  T40 = "NHID_40", # 40 테이블명 (상병내역)
  T60 = "NHID_60", # 60 테이블명 (처방전상세)
  GJ = "NHID_GJ",   # 건강검진(Gumjin) 테이블명
  YK = "NHID_YK"    # 요양기관(Yoyang) 테이블명
)

# 6. ETL 함수 실행
executeNHISETL(
  # --- 데이터베이스 스키마 설정 ---
  NHISNSC_rawdata = "nhisnsc2013original.dbo",    # 원본 데이터가 있는 스키마 (DB명.dbo)
  NHISNSC_database = "nhisnsc2013cdm.dbo",   # 결과가 저장될 스키마
  Mapping_database = "nhisnsc2013cdm.dbo",   # 매핑 테이블이 있는 스키마 (보통 CDM과 동일하게 설정)
  vocaFolder = "C:/Users/chaeyoon/Desktop/koreajapan/vocabulary",
  
  # --- 원본 테이블 이름 설정 (위에서 정의한 list 사용) ---
  NHIS_JK = nhis_tables$JK,
  NHIS_20T = nhis_tables$T20,
  NHIS_30T = nhis_tables$T30,
  NHIS_40T = nhis_tables$T40,
  NHIS_60T = nhis_tables$T60,
  NHIS_GJ = nhis_tables$GJ,
  NHIS_YK = nhis_tables$YK,
  
  # --- 연결 객체 및 로그 폴더 ---
  connection = conn, 
  outputFolder = "C:/Users/chaeyoon/Desktop/koreajapan/nhis_study/ETL_Logs",  
  
  # --- 실행 단계 제어 (TRUE/FALSE) ---
  # 처음 돌릴 때는 CDM_ddl = TRUE로 테이블을 만들어야 합니다.
  CDM_ddl = FALSE,              # CDM 빈 테이블 생성
  master_table = FALSE,         # 마스터 시퀀스 테이블 생성
  import_voca = FALSE,
  
  # 데이터 적재 (필요한 부분만 TRUE로 변경 가능)
  location = FALSE,
  care_site = FALSE,
  person = FALSE,
  death = TRUE,
  observation_period = FALSE,
  visit_occurrence = FALSE,
  condition_occurrence = TRUE,
  observation = TRUE,
  drug_exposure = TRUE,
  procedure_occurrence = TRUE,
  device_exposure = TRUE,
  measurement = TRUE,
  payer_plan_period = FALSE,
  cost = FALSE,
  
  # 후처리
  generateEra = FALSE,          # Era 테이블 생성 (시간 오래 걸림)
  dose_era = FALSE,
  cdm_source = FALSE,
  indexing = FALSE,             # 인덱스 생성 (필수)
  constraints = FALSE,          # 제약조건 생성 (필수)
  data_cleansing = FALSE       # 데이터 클렌징 (선택 사항)
)

# 7. 연결 종료
disconnect(conn)