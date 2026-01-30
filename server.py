import pyodbc

# SQL Server 연결 문자열 설정
server = 'DESKTOP-HBA9S76\\SQLEXPRESS01'  # 서버 이름 및 인스턴스
database = 'master'  # master 데이터베이스로 연결하여 새 데이터베이스를 생성합니다
username = ''  # Windows 인증 사용 시 비워두기
password = ''  # Windows 인증 사용 시 비워두기

# ODBC 연결 문자열 (Windows 인증 사용)
connection_string = f'DRIVER={{ODBC Driver 18 for SQL Server}};' \
                    f'SERVER={server};' \
                    f'DATABASE={database};' \
                    f'Trusted_Connection=Yes;' \
                    f'Encrypt=no;'  # SSL 연결 비활성화

try:
    # 연결 시도
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()

    # 데이터베이스가 없다면 생성
    cursor.execute("IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'nhisnsc2013cdm') "
                   "CREATE DATABASE nhisnsc2013cdm")
    
    # 커밋 없이 바로 데이터베이스 생성
    conn.commit()  # 커밋하여 데이터베이스 생성 완료

    print("nhisnsc2013cdm 데이터베이스가 성공적으로 생성되었습니다.")

    # 연결 종료
    cursor.close()
    conn.close()

except Exception as e:
    print(f"SQL Server 연결 실패: {e}")
