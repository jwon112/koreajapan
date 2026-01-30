# odbc 패키지 로드
library(odbc)

# SQL Server 연결 설정
con <- dbConnect(odbc(),
                 Driver = "ODBC Driver 17 for SQL Server",  # 설치된 ODBC 드라이버
                 Server = "DESKTOP-HBA9S76\\SQLEXPRESS01",   # SQL Server 인스턴스
                 Database = "master",  # master 데이터베이스로 연결하여 새 데이터베이스 생성
                 Trusted_Connection = "Yes",  # Windows 인증
                 Encrypt = "no")  # SSL 비활성화

# 데이터베이스가 없으면 생성
query <- "IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'nhisnsc2013cdm') 
          CREATE DATABASE nhisnsc2013cdm"

# SQL 실행
dbExecute(con, query)

# 연결 종료
dbDisconnect(con)

cat("nhisnsc2013cdm 데이터베이스가 성공적으로 생성되었습니다.\n")
