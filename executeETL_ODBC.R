executeNHISETL_ODBC <- function(
        NHISNSC_rawdata,
        NHISNSC_database,
        Mapping_database,
        NHIS_JK,
        NHIS_20T,
        NHIS_30T,
        NHIS_40T,
        NHIS_60T,
        NHIS_GJ,
        NHIS_YK,
        
        connection,        # ODBC 연결
        outputFolder,
        
        CDM_ddl = FALSE,
        master_table = FALSE,
        location = FALSE,
        care_site = FALSE,
        person = FALSE,
        death = FALSE,
        observation_period = FALSE,
        visit_occurrence = FALSE,
        condition_occurrence = FALSE,
        observation = FALSE,
        drug_exposure = TRUE,
        procedure_occurrence = TRUE,
        device_exposure = TRUE,
        measurement = TRUE,
        payer_plan_period = TRUE,
        cost = TRUE,
        generateEra = TRUE,
        dose_era = TRUE,
        cdm_source = TRUE,
        indexing = TRUE,
        constraints = TRUE,
        data_cleansing = TRUE
){
    
    # 고정 DBMS
    targetDialect <- "sql server"
    
    logFile <- file.path(outputFolder, "log.txt")
    ParallelLogger::addDefaultFileLogger(logFile)
    
    runSql <- function(sql) {
        sql2 <- SqlRender::translate(sql, targetDialect = targetDialect)
        DBI::dbExecute(connection, sql2)
    }
    
    querySql <- function(sql) {
        sql2 <- SqlRender::translate(sql, targetDialect = targetDialect)
        DBI::dbGetQuery(connection, sql2)
    }
    
    loadSqlFile <- function(file, ...) {
        SqlRender::loadRenderTranslateSql(
            sqlFilename = file,
            packageName = "etlKoreanNSC",
            dbms = targetDialect,
            ...
        )
    }
    
    # ----------------------
    # 0. CDM DDL
    # ----------------------
    if (CDM_ddl) {
        ParallelLogger::logInfo("Creating empty CDM tables...")
        
        SqlFile <- "000.OMOP CDM sql server ddl.sql"
        dbOnly <- strsplit(NHISNSC_database, "\\.")[[1]][1]
        
        sql <- loadSqlFile(SqlFile, NHISNSC_database = dbOnly)
        runSql(sql)
        
        ParallelLogger::logInfo("CDM tables created.")
    }
    
    # ----------------------
    # Example block: care_site
    # ----------------------
    if (care_site) {
        ParallelLogger::logInfo("ETL 030.Care_site starting")
        
        SqlFile <- "030.Care_site.sql"
        
        sql <- loadSqlFile(
            SqlFile,
            NHISNSC_rawdata = NHISNSC_rawdata,
            NHISNSC_database = NHISNSC_database,
            NHIS_YK = NHIS_YK
        )
        
        runSql(sql)
        
        cnt <- querySql(
            SqlRender::render(
                "SELECT COUNT(*) AS cnt FROM @db.care_site",
                db = NHISNSC_database
            )
        )
        
        ParallelLogger::logInfo(paste("care_site rows:", cnt$cnt))
    }
    
    # ----------------------
    # Example block: observation_period
    # ----------------------
    if (observation_period) {
        ParallelLogger::logInfo("ETL 060.Observation_period starting")
        
        SqlFile <- "060.Observation_period.sql"
        
        sql <- loadSqlFile(
            SqlFile,
            NHISNSC_rawdata = NHISNSC_rawdata,
            NHISNSC_database = NHISNSC_database,
            NHIS_JK = NHIS_JK
        )
        
        runSql(sql)
        
        cnt <- querySql(
            SqlRender::render(
                "SELECT COUNT(*) AS cnt FROM @db.observation_period",
                db = NHISNSC_database
            )
        )
        
        ParallelLogger::logInfo(paste("observation_period rows:", cnt$cnt))
    }
    
    ParallelLogger::logInfo("ETL 완료 (ODBC 버전).")
}
