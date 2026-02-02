#' DQevaluation: 매핑 검수 (원본 코드 vs source_to_concept_map Mappied/Unmappied 건수)
#'
#' @param NHISNSC_rawdata 원본 스키마 (DB.dbo)
#' @param NHISNSC_database CDM 스키마 (seq_master 등)
#' @param Mapping_database 매핑 테이블 스키마 (source_to_concept_map)
#' @param NHIS_JK,NHIS_20T,NHIS_30T,NHIS_40T,NHIS_60T,NHIS_GJ,NHIS_YK 원본 테이블명
#' @param GJ_vertical GJ UNPIVOT 뷰/테이블명 (090.Observation.sql에서 생성, 없으면 measurement=FALSE로 두거나 해당 테이블 생성 후 전달)
#' @param connection DB 연결 객체
#' @param outputFolder 로그 경로
#' @param drug_exposure,procedure_occurrence,device_exposure,condition_occurrence,measurement 도메인별 검수 여부
#' @return 도메인별 매핑/비매핑 건수 등이 담긴 리스트 (콘솔에 요약 출력 후 invisible 반환)
#' @export
DQevaluation <- function(NHISNSC_rawdata,
                         NHISNSC_database,
                         Mapping_database,
                         NHIS_JK,
                         NHIS_20T,
                         NHIS_30T,
                         NHIS_40T,
                         NHIS_60T,
                         NHIS_GJ,
                         NHIS_YK,
                         GJ_vertical = "gj_vertical",
                         
                         connection,
                         outputFolder,
                         
                         drug_exposure = TRUE,
                         procedure_occurrence = TRUE,
                         device_exposure = TRUE,
                         condition_occurrence = TRUE,
                         measurement = TRUE
                         
                         
){
    ## DB/테이블명 파라미터 (SqlRender render용)
    renderParams <- list(
        NHISNSC_rawdata = NHISNSC_rawdata,
        NHISNSC_database = NHISNSC_database,
        Mapping_database = Mapping_database,
        NHIS_20T = NHIS_20T,
        NHIS_30T = NHIS_30T,
        NHIS_40T = NHIS_40T,
        NHIS_60T = NHIS_60T,
        NHIS_GJ = NHIS_GJ,
        NHIS_YK = NHIS_YK,
        GJ_vertical = GJ_vertical
    )
    
    DQresults <- list()
    
    ## Drug_exposure
    if(drug_exposure){
        
        ## Mapping Table
        SqlMapping <- c("
                        IF Object_id('tempdb..#mapping_table', 'U') IS NOT NULL 
                        DROP TABLE #mapping_table; 
                        SELECT a.source_code, a.target_concept_id, a.domain_id, Replace(a.invalid_reason, '', NULL) AS invalid_reason 
                        INTO   #mapping_table 
                        FROM   @Mapping_database.source_to_concept_map a 
                        JOIN @Mapping_database.concept b 
                        ON a.target_concept_id = b.concept_id 
                        WHERE  a.invalid_reason IS NULL 
                        AND b.invalid_reason IS NULL 
                        AND a.domain_id = 'drug';
                        ")
        sql <- do.call(SqlRender::render, c(list(SqlMapping), renderParams))
        DatabaseConnector::executeSql(connection, sql)
        
        
        ## 30T Mappied
        SqlMappied30T <- c("
                           SELECT multimappied, Count(*) as count -- 104,292,115
                           FROM   (SELECT master_seq, Count(*) AS multimappied -- 104,292,115 
                           FROM   (SELECT master_seq, div_cd 
                           FROM   @NHISNSC_rawdata.@NHIS_30T x, 
                           (SELECT master_seq, person_id, key_seq, seq_no 
                           FROM   @NHISNSC_database.SEQ_MASTER 
                           WHERE  source_table = '130') y, 
                           @NHISNSC_rawdata.@NHIS_20T z 
                           WHERE  x.key_seq = y.key_seq 
                           AND x.seq_no = y.seq_no 
                           AND y.key_seq = z.key_seq 
                           AND y.person_id = z.person_id) a, 
                           #mapping_table b 
                           WHERE  a.div_cd = b.source_code 
                           GROUP  BY master_seq) c 
                           GROUP  BY multimappied 
                           ")
        sql <- do.call(SqlRender::render, c(list(SqlMappied30T), renderParams))
        ConvertedDrugCountByMappied30T <- DatabaseConnector::querySql(connection, sql)
        
        ## 30T Unmappied
        SqlUnMappied30T <- c("
                             SELECT Count(*) -- 4,400,052
                             FROM   (SELECT master_seq, div_cd 
                             FROM   (SELECT * 
                             FROM   @NHISNSC_rawdata.@NHIS_30T 
                             WHERE  div_type_cd IN ( '3', '4', '5' )) x, 
                             (SELECT master_seq, person_id, key_seq, seq_no 
                             FROM   @NHISNSC_database.SEQ_MASTER 
                             WHERE  source_table = '130') y, 
                             @NHISNSC_rawdata.@NHIS_20T z 
                             WHERE  x.key_seq = y.key_seq 
                             AND x.seq_no = y.seq_no 
                             AND y.key_seq = z.key_seq 
                             AND y.person_id = z.person_id) a 
                             WHERE  a.div_cd NOT IN (SELECT source_code 
                             FROM   #mapping_table)
                             ")
        sql <- do.call(SqlRender::render, c(list(SqlUnMappied30T), renderParams))
        ConvertedDrugCountByUnMappied30T <- DatabaseConnector::querySql(connection, sql)
        
        ## 30T Raw
        SqlRawToDrugBy30T_1 <- c("
                                 SELECT div_type_cd, multimappied, Count(*) AS count 
                                 FROM   (SELECT key_seq, seq_no, div_type_cd, Count(*) AS multimappied 
                                 FROM   (SELECT * 
                                 FROM   (SELECT * 
                                 FROM   @NHISNSC_rawdata.@NHIS_30T 
                                 WHERE  div_type_cd NOT IN ( '3', '4', '5' )) a 
                                 JOIN #mapping_table b 
                                 ON a.div_cd = b.source_code) c -- 1:1 mapping  
                                 GROUP  BY key_seq, seq_no, div_type_cd) d 
                                 GROUP  BY div_type_cd, multimappied 
                                 ")
        SqlRawToDrugBy30T_2 <- c("
                                 SELECT div_type_cd, 1 as multimappied, Count(*) as count 
                                 FROM   (SELECT * 
                                 FROM   @NHISNSC_rawdata.@NHIS_30T 
                                 WHERE  div_type_cd IN ( '3', '4', '5' )) a
                                 GROUP by div_type_cd
                                 ")
        sql <- do.call(SqlRender::render, c(list(SqlRawToDrugBy30T_1), renderParams))
        HowManyContainDrugByMappied30T <- DatabaseConnector::querySql(connection, sql)
        sql <- do.call(SqlRender::render, c(list(SqlRawToDrugBy30T_2), renderParams))
        tmp <- DatabaseConnector::querySql(connection, sql)
        names(tmp) <- names(HowManyContainDrugByMappied30T)
        HowManyContainDrugByMappied30T <- rbind(HowManyContainDrugByMappied30T, tmp)
        
        
        ## 60T Mappied
        SqlMappied60T <- c("
                           SELECT multimappied, Count(*) -- 384,321,194
                           FROM   (SELECT master_seq, Count(*) AS multimappied -- 
                           FROM   (SELECT master_seq, div_cd 
                           FROM   @NHISNSC_rawdata.@NHIS_60T x, 
                           (SELECT master_seq, person_id, key_seq, seq_no 
                           FROM   @NHISNSC_database.SEQ_MASTER 
                           WHERE  source_table = '160') y, 
                           @NHISNSC_rawdata.@NHIS_20T z 
                           WHERE  x.key_seq = y.key_seq 
                           AND x.seq_no = y.seq_no 
                           AND y.key_seq = z.key_seq 
                           AND y.person_id = z.person_id) a, 
                           #mapping_table b 
                           WHERE  a.div_cd = b.source_code 
                           GROUP  BY master_seq) c 
                           GROUP  BY multimappied
                           ")
        sql <- do.call(SqlRender::render, c(list(SqlMappied60T), renderParams))
        ConvertedDrugCountByMappied60T <- DatabaseConnector::querySql(connection, sql)
        
        ## 60T Unmappied
        SqlUnMappied60T <- c("
                             SELECT Count(*) -- 
                             FROM   (SELECT master_seq, div_cd 
                             FROM   (SELECT * 
                             FROM   @NHISNSC_rawdata.@NHIS_60T 
                             WHERE  div_type_cd IN ( '3', '4', '5' )) x, 
                             (SELECT master_seq, person_id, key_seq, seq_no 
                             FROM   @NHISNSC_database.SEQ_MASTER 
                             WHERE  source_table = '160') y, 
                             @NHISNSC_rawdata.@NHIS_20T z 
                             WHERE  x.key_seq = y.key_seq 
                             AND x.seq_no = y.seq_no 
                             AND y.key_seq = z.key_seq 
                             AND y.person_id = z.person_id) a 
                             WHERE  a.div_cd NOT IN (SELECT source_code 
                             FROM   #mapping_table) 
                             ")
        sql <- do.call(SqlRender::render, c(list(SqlUnMappied60T), renderParams))
        ConvertedDrugCountByUnMappied60T <- DatabaseConnector::querySql(connection, sql)
        
        ## 60T Raw
        SqlRawToDrugBy60T_1 <- c("
                                 SELECT div_type_cd, multimappied, Count(*) AS count 
                                 FROM   (SELECT key_seq, seq_no, div_type_cd, Count(*) AS multimappied 
                                 FROM   (SELECT * 
                                 FROM   (SELECT * 
                                 FROM   @NHISNSC_rawdata.@NHIS_60T 
                                 WHERE  div_type_cd NOT IN ( '3', '4', '5' )) a 
                                 JOIN #mapping_table b 
                                 ON a.div_cd = b.source_code) c -- 1:1 mapping  
                                 GROUP  BY key_seq, seq_no, div_type_cd) d 
                                 GROUP  BY div_type_cd, multimappied
                                 ")
        SqlRawToDrugBy60T_2 <- c("
                                 SELECT div_type_cd, 1 as multimappied, Count(*) as COUNT 
                                 FROM   (SELECT * 
                                 FROM   @NHISNSC_rawdata.@NHIS_60T 
                                 WHERE  div_type_cd IN ( '3', '4', '5' )) a
                                 GROUP by div_type_cd
                                 ")
        sql <- do.call(SqlRender::render, c(list(SqlRawToDrugBy60T_1), renderParams))
        HowManyContainDrugByMappied60T <- DatabaseConnector::querySql(connection, sql)
        sql <- do.call(SqlRender::render, c(list(SqlRawToDrugBy60T_2), renderParams))
        tmp <- DatabaseConnector::querySql(connection, sql)
        names(tmp) <- names(HowManyContainDrugByMappied60T)
        HowManyContainDrugByMappied60T <- rbind(HowManyContainDrugByMappied60T, tmp)
        
        DQresults$drug_exposure <- list(
            Mapped_30T = ConvertedDrugCountByMappied30T,
            Unmapped_30T = ConvertedDrugCountByUnMappied30T,
            Mapped_60T = ConvertedDrugCountByMappied60T,
            Unmapped_60T = ConvertedDrugCountByUnMappied60T,
            Raw_30T = HowManyContainDrugByMappied30T,
            Raw_60T = HowManyContainDrugByMappied60T
        )
        
    } ## DrugTable : 20T join으로 인해 4건 차이남, Death기간으로 인해 delete 발생, Cost 적재시 delete 발생
    
    
    
    
    ## Procedure_occurrence
    if(procedure_occurrence){
        
        ## Mapping Table
        SqlMapping <- c("
                        IF OBJECT_ID('tempdb..#mapping_table', 'U') IS NOT NULL
                        DROP TABLE #mapping_table;
                        IF OBJECT_ID('tempdb..#temp', 'U') IS NOT NULL
                        DROP TABLE #temp;
                        IF OBJECT_ID('tempdb..#duplicated', 'U') IS NOT NULL
                        DROP TABLE #duplicated;
                        IF OBJECT_ID('tempdb..#pro', 'U') IS NOT NULL
                        DROP TABLE #pro;
                        IF OBJECT_ID('tempdb..#five', 'U') IS NOT NULL
                        DROP TABLE #five;
                        select a.source_code, a.target_concept_id, a.domain_id, REPLACE(a.invalid_reason, '', NULL) as invalid_reason
                        into #temp
                        from @Mapping_database.source_to_concept_map a join @Mapping_database.CONCEPT b on a.target_concept_id=b.concept_id
                        where a.invalid_reason is null and b.invalid_reason is null and a.domain_id='procedure';
                        
                        select * into #pro from @Mapping_database.source_to_concept_map where domain_id='procedure';
                        select * into #five from @Mapping_database.source_to_concept_map where domain_id='device';
                        
                        select a.*
                        into #duplicated
                        from #pro a, #five b
                        where a.source_code=b.source_code
                        and a.invalid_reason is null and b.invalid_reason is null;
                        
                        select * into #mapping_table from #temp
                        where source_code not in (select source_code from #duplicated);
                        
                        drop table #pro, #five, #temp;
                        ")
        sql <- do.call(SqlRender::render, c(list(SqlMapping), renderParams))
        DatabaseConnector::executeSql(connection, sql)
        
        
        ## 30T Mappied
        SqlMappied30T <- c("
                           SELECT Count(*) -- 234,624,188
                           FROM   (SELECT x.div_cd, x.div_type_cd 
                           FROM   (SELECT * 
                           FROM   @NHISNSC_rawdata.@NHIS_30T 
                           WHERE  div_type_cd NOT IN ( '3', '4', '5', '7', '8' )) x, 
                           (SELECT * FROM   @NHISNSC_database.SEQ_MASTER 
                           WHERE  source_table = '130') y 
                           WHERE  x.key_seq = y.key_seq 
                           AND x.seq_no = y.seq_no) a, 
                           #mapping_table b -- 1:n mappied
                           WHERE  LEFT(a.div_cd, 5) = b.source_code
                           ")
        sql <- do.call(SqlRender::render, c(list(SqlMappied30T), renderParams))
        ConvertedProcCountByMappied30T <- DatabaseConnector::querySql(connection, sql)
        
        ## 30T Dup Mappied
        SqlDupMappied30T <- c("
                              SELECT Count(*) -- 3,448,362 
                              FROM   (SELECT x.div_cd, x.div_type_cd 
                              FROM   (SELECT * 
                              FROM   @NHISNSC_rawdata.@NHIS_30T 
                              WHERE  div_type_cd IN ( '1', '2' )) x, 
                              (SELECT * FROM   @NHISNSC_database.SEQ_MASTER 
                              WHERE  source_table = '130') y 
                              WHERE  x.key_seq = y.key_seq 
                              AND x.seq_no = y.seq_no) a, 
                              #duplicated b 
                              WHERE  LEFT(a.div_cd, 5) = b.source_code
                              ")
        sql <- do.call(SqlRender::render, c(list(SqlDupMappied30T), renderParams))
        ConvertedProcCountByDupMappied30T <- DatabaseConnector::querySql(connection, sql)
        
        ## 30T UnMappied
        SqlUnMappied30T <- c("
                             SELECT Count(*) -- 214,373,129
                             FROM   (SELECT x.div_cd, x.div_type_cd 
                             FROM   (SELECT * 
                             FROM   @NHISNSC_rawdata.@NHIS_30T 
                             WHERE  div_type_cd IN ( '1', '2' )) x, 
                             (SELECT *
                             FROM   @NHISNSC_database.SEQ_MASTER 
                             WHERE  source_table = '130') y 
                             WHERE  x.key_seq = y.key_seq 
                             AND x.seq_no = y.seq_no) a 
                             WHERE  LEFT(a.div_cd, 5) NOT IN (SELECT source_code 
                             FROM   #duplicated 
                             UNION ALL 
                             SELECT source_code 
                             FROM   #mapping_table)
                             ")
        sql <- do.call(SqlRender::render, c(list(SqlUnMappied30T), renderParams))
        ConvertedProcCountByUnMappied30T <- DatabaseConnector::querySql(connection, sql)
        
        ## 30T Raw
        SqlRawToProcBy30T_1 <- c("
                                 SELECT div_type_cd, multimappied, Count(*) AS COUNT 
                                 FROM   (SELECT key_seq, seq_no, div_type_cd, Count(*) AS multimappied 
                                 FROM   (SELECT * 
                                 FROM   (SELECT * 
                                 FROM   @NHISNSC_rawdata.@NHIS_30T 
                                 WHERE  div_type_cd NOT IN ( '3', '4', '5', '7', '8' )) a, #mapping_table b 
                                 WHERE  LEFT(a.div_cd, 5) = b.source_code) c 
                                 GROUP  BY key_seq, seq_no, div_type_cd) d 
                                 GROUP  BY div_type_cd, multimappied 
                                 ")
        SqlRawToProcBy30T_2 <- c("
                                 SELECT div_type_cd, multimappied, Count(*) AS COUNT 
                                 FROM   (SELECT key_seq, seq_no, div_type_cd, Count(*) AS multimappied 
                                 FROM   (SELECT * 
                                 FROM   (SELECT * 
                                 FROM   @NHISNSC_rawdata.@NHIS_30T 
                                 WHERE  div_type_cd IN ( '1', '2' )) a, #duplicated b 
                                 WHERE  LEFT(a.div_cd, 5) = b.source_code) c 
                                 GROUP  BY key_seq, seq_no, div_type_cd) d 
                                 GROUP  BY div_type_cd, multimappied 
                                 ")
        SqlRawToProcBy30T_3 <- c("
                                 SELECT div_type_cd, multimappied, Count(*) AS COUNT 
                                 FROM   (SELECT key_seq, seq_no, div_type_cd, Count(*) AS multimappied 
                                 FROM   (SELECT * 
                                 FROM   (SELECT * 
                                 FROM   @NHISNSC_rawdata.@NHIS_30T 
                                 WHERE  div_type_cd IN ( '1', '2' )) a
                                 WHERE  LEFT(a.div_cd, 5) NOT IN (SELECT source_code FROM #duplicated UNION ALL SELECT source_code FROM #mapping_table)) c 
                                 GROUP  BY key_seq, seq_no, div_type_cd) d 
                                 GROUP  BY div_type_cd, multimappied 
                                 ")
        sql <- do.call(SqlRender::render, c(list(SqlRawToProcBy30T_1), renderParams))
        HowManyContainProcByMappied30T <- DatabaseConnector::querySql(connection, sql)
        sql <- do.call(SqlRender::render, c(list(SqlRawToProcBy30T_2), renderParams))
        tmp <- DatabaseConnector::querySql(connection, sql)
        names(tmp) <- names(HowManyContainProcByMappied30T)
        HowManyContainProcByMappied30T <- rbind(HowManyContainProcByMappied30T, tmp)
        sql <- do.call(SqlRender::render, c(list(SqlRawToProcBy30T_3), renderParams))
        tmp <- DatabaseConnector::querySql(connection, sql)
        names(tmp) <- names(HowManyContainProcByMappied30T)
        HowManyContainProcByMappied30T <- rbind(HowManyContainProcByMappied30T, tmp)
        
        
        ## 60T Mappied
        SqlMappied60T <- c("
                           SELECT Count(*) -- 8,785
                           FROM   (SELECT x.div_cd, x.div_type_cd 
                           FROM   (SELECT * 
                           FROM   @NHISNSC_rawdata.@NHIS_60T 
                           WHERE  div_type_cd NOT IN ( '3', '4', '5', '7', '8' )) x, 
                           (SELECT * 
                           FROM   @NHISNSC_database.SEQ_MASTER 
                           WHERE  source_table = '160') y 
                           WHERE  x.key_seq = y.key_seq 
                           AND x.seq_no = y.seq_no) a, 
                           #mapping_table b 
                           WHERE  LEFT(a.div_cd, 5) = b.source_code
                           ")
        sql <- do.call(SqlRender::render, c(list(SqlMappied60T), renderParams))
        ConvertedProcCountByMappied60T <- DatabaseConnector::querySql(connection, sql)
        
        ## 60T Dup Mappied
        SqlDupMappied60T <- c("
                              SELECT Count(*) -- 5 
                              FROM   (SELECT x.div_cd, x.div_type_cd 
                              FROM   (SELECT * 
                              FROM   @NHISNSC_rawdata.@NHIS_60T 
                              WHERE  div_type_cd IN ( '1', '2' )) x, 
                              (SELECT *
                              FROM   @NHISNSC_database.SEQ_MASTER 
                              WHERE  source_table = '160') y 
                              WHERE  x.key_seq = y.key_seq 
                              AND x.seq_no = y.seq_no) a, 
                              #duplicated b 
                              WHERE  LEFT(a.div_cd, 5) = b.source_code 
                              ")
        sql <- do.call(SqlRender::render, c(list(SqlDupMappied60T), renderParams))
        ConvertedProcCountByDupMappied60T <- DatabaseConnector::querySql(connection, sql)
        
        ## 60T UnMappied
        SqlUnMappied <- c("
                          SELECT Count(*) -- 25,286
                          FROM   (SELECT x.div_cd, x.div_type_cd 
                          FROM   (SELECT * 
                          FROM   @NHISNSC_rawdata.@NHIS_60T 
                          WHERE  div_type_cd IN ( '1', '2' )) x, 
                          (SELECT *
                          FROM   @NHISNSC_database.SEQ_MASTER 
                          WHERE  source_table = '160') y 
                          WHERE  x.key_seq = y.key_seq 
                          AND x.seq_no = y.seq_no) a 
                          WHERE  LEFT(a.div_cd, 5) NOT IN (SELECT source_code 
                          FROM   #duplicated 
                          UNION ALL 
                          SELECT source_code 
                          FROM   #mapping_table)
                          ")
        sql <- do.call(SqlRender::render, c(list(SqlUnMappied), renderParams))
        ConvertedProcCountByUnMappied60T <- DatabaseConnector::querySql(connection, sql)
        
        ## 60T Raw
        SqlRawToProcBy60T_1 <- c("
                                 SELECT div_type_cd, multimappied, Count(*) AS COUNT 
                                 FROM   (SELECT key_seq, seq_no, div_type_cd, Count(*) AS multimappied 
                                 FROM   (SELECT * 
                                 FROM   (SELECT * 
                                 FROM   @NHISNSC_rawdata.@NHIS_60T 
                                 WHERE  div_type_cd NOT IN ( '3', '4', '5', '7', '8' )) a, #mapping_table b 
                                 WHERE  LEFT(a.div_cd, 5) = b.source_code) c 
                                 GROUP  BY key_seq, seq_no, div_type_cd) d 
                                 GROUP  BY div_type_cd, multimappied 
                                 ")
        SqlRawToProcBy60T_2 <- c("
                                 SELECT div_type_cd, multimappied, Count(*) AS COUNT 
                                 FROM   (SELECT key_seq, seq_no, div_type_cd, Count(*) AS multimappied 
                                 FROM   (SELECT * 
                                 FROM   (SELECT * 
                                 FROM   @NHISNSC_rawdata.@NHIS_60T 
                                 WHERE  div_type_cd IN ( '1', '2' )) a, #duplicated b 
                                 WHERE  LEFT(a.div_cd, 5) = b.source_code) c 
                                 GROUP  BY key_seq, seq_no, div_type_cd) d 
                                 GROUP  BY div_type_cd, multimappied 
                                 ")
        SqlRawToProcBy60T_3 <- c("
                                 SELECT div_type_cd, multimappied, Count(*) AS COUNT 
                                 FROM   (SELECT key_seq, seq_no, div_type_cd, Count(*) AS multimappied 
                                 FROM   (SELECT * 
                                 FROM   (SELECT * 
                                 FROM   @NHISNSC_rawdata.@NHIS_60T 
                                 WHERE  div_type_cd IN ( '1', '2' )) a
                                 WHERE  LEFT(a.div_cd, 5) NOT IN (SELECT source_code FROM #duplicated UNION ALL SELECT source_code FROM #mapping_table)) c 
                                 GROUP  BY key_seq, seq_no, div_type_cd) d 
                                 GROUP  BY div_type_cd, multimappied 
                                 ")
        sql <- do.call(SqlRender::render, c(list(SqlRawToProcBy60T_1), renderParams))
        HowManyContainProcByMappied60T <- DatabaseConnector::querySql(connection, sql)
        sql <- do.call(SqlRender::render, c(list(SqlRawToProcBy60T_2), renderParams))
        tmp <- DatabaseConnector::querySql(connection, sql)
        names(tmp) <- names(HowManyContainProcByMappied60T)
        HowManyContainProcByMappied60T <- rbind(HowManyContainProcByMappied60T, tmp)
        sql <- do.call(SqlRender::render, c(list(SqlRawToProcBy60T_3), renderParams))
        tmp <- DatabaseConnector::querySql(connection, sql)
        names(tmp) <- names(HowManyContainProcByMappied60T)
        HowManyContainProcByMappied60T <- rbind(HowManyContainProcByMappied60T, tmp)
        
        DQresults$procedure_occurrence <- list(
            Mapped_30T = ConvertedProcCountByMappied30T,
            DupMapped_30T = ConvertedProcCountByDupMappied30T,
            Unmapped_30T = ConvertedProcCountByUnMappied30T,
            Mapped_60T = ConvertedProcCountByMappied60T,
            DupMapped_60T = ConvertedProcCountByDupMappied60T,
            Unmapped_60T = ConvertedProcCountByUnMappied60T,
            Raw_30T = HowManyContainProcByMappied30T,
            Raw_60T = HowManyContainProcByMappied60T
        )
        
    }
    
    
    
    
    ## Device_exposure
    if(device_exposure){
        
        ## Mapping Table
        SqlMapping <- c("
                        IF OBJECT_ID('tempdb..#mapping_table', 'U') IS NOT NULL
                        DROP TABLE #mapping_table;
                        IF OBJECT_ID('tempdb..#temp', 'U') IS NOT NULL
                        DROP TABLE #temp;
                        IF OBJECT_ID('tempdb..#duplicated', 'U') IS NOT NULL
                        DROP TABLE #duplicated;
                        IF OBJECT_ID('tempdb..#device', 'U') IS NOT NULL
                        DROP TABLE #device;
                        IF OBJECT_ID('tempdb..#five', 'U') IS NOT NULL
                        DROP TABLE #five;
                        
                        select a.source_code, a.target_concept_id, a.domain_id, REPLACE(a.invalid_reason, '', NULL) as invalid_reason
                        into #temp
                        from @Mapping_database.source_to_concept_map a join @Mapping_database.CONCEPT b on a.target_concept_id=b.concept_id
                        where a.invalid_reason is null and b.invalid_reason is null and a.domain_id='device';
                        
                        select * into #device from @Mapping_database.source_to_concept_map where domain_id='device';
                        select * into #five from @Mapping_database.source_to_concept_map where domain_id='procedure';
                        
                        select a.*
                        into #duplicated
                        from #device a, #five b
                        where a.source_code=b.source_code
                        and a.invalid_reason is null and b.invalid_reason is null;
                        
                        select * into #mapping_table from #temp
                        where source_code not in (select source_code from #duplicated);
                        
                        drop table #device, #five, #temp;
                        ")
        sql <- do.call(SqlRender::render, c(list(SqlMapping), renderParams))
        DatabaseConnector::executeSql(connection, sql)
        
        
        ## 30T Mappied
        SqlMappied30T <- c("
                           SELECT Count(*) -- 7,886,009
                           FROM   (SELECT x.key_seq, x.div_cd 
                           FROM   (SELECT * 
                           FROM   @NHISNSC_rawdata.@NHIS_30T 
                           WHERE  div_type_cd NOT IN ( '1', '2', '3', '4', '5' )) x, 
                           @NHISNSC_database.SEQ_MASTER y 
                           WHERE  y.source_table = '130' 
                           AND x.key_seq = y.key_seq 
                           AND x.seq_no = y.seq_no) a 
                           JOIN #mapping_table b 
                           ON a.div_cd = b.source_code; 
                           ")
        sql <- do.call(SqlRender::render, c(list(SqlMappied30T), renderParams))
        ConvertedDeviCountByMappied30T <- DatabaseConnector::querySql(connection, sql)
        
        ## 30T Dup Mappied
        SqlDupMappied30T <- c("
                              SELECT Count(*) --1,016
                              FROM   (SELECT x.key_seq, 
                              x.div_cd 
                              FROM   (SELECT * 
                              FROM   @NHISNSC_rawdata.@NHIS_30T 
                              WHERE  div_type_cd IN ( '7', '8' )) x, 
                              @NHISNSC_database.SEQ_MASTER y 
                              WHERE  y.source_table = '130' 
                              AND x.key_seq = y.key_seq 
                              AND x.seq_no = y.seq_no) a 
                              JOIN #duplicated b 
                              ON a.div_cd = b.source_code
                              ")
        sql <- do.call(SqlRender::render, c(list(SqlDupMappied30T), renderParams))
        ConvertedDeviCountByDupMappied30T <- DatabaseConnector::querySql(connection, sql)
        
        ## 30T UnMappied
        SqlUnMappied <- c("
                          SELECT Count(*) -- 3,493,993
                          FROM   (SELECT x.key_seq, 
                          x.div_cd 
                          FROM   (SELECT * 
                          FROM   @NHISNSC_rawdata.@NHIS_30T 
                          WHERE  div_type_cd IN ( '7', '8' )) x, 
                          @NHISNSC_database.SEQ_MASTER y 
                          WHERE  y.source_table = '130' 
                          AND x.key_seq = y.key_seq 
                          AND x.seq_no = y.seq_no) a 
                          WHERE  a.div_cd NOT IN (SELECT source_code 
                          FROM   #duplicated 
                          UNION ALL 
                          SELECT source_code 
                          FROM   #mapping_table); 
                          ")
        sql <- do.call(SqlRender::render, c(list(SqlUnMappied), renderParams))
        ConvertedDeviCountByUnMappied30T <- DatabaseConnector::querySql(connection, sql)
        
        ## 30T Raw
        SqlRawToDeviBy30T_1 <- c("
                                 SELECT div_type_cd, multimappied, Count(*) AS COUNT 
                                 from (select key_seq, seq_no, div_type_cd, count(*) as multimappied
                                 FROM   (SELECT * 
                                 FROM   (SELECT * 
                                 FROM   @NHISNSC_rawdata.@NHIS_30T 
                                 WHERE  div_type_cd NOT IN ( '1', '2', '3', '4', '5', '7', '8' )) x 
                                 JOIN #mapping_table y -- 1:1 mapping 
                                 ON x.div_cd = y.source_code) z 
                                 group by key_seq, seq_no, div_type_cd) a
                                 group by div_type_cd, multimappied
                                 ")
        SqlRawToDeviBy30T_2 <- c("
                                 SELECT div_type_cd, 1 as multimappied, Count(*) AS COUNT
                                 FROM   (SELECT * 
                                 FROM   @NHISNSC_rawdata.@NHIS_30T 
                                 WHERE  div_type_cd IN ( '7', '8' )) x 
                                 GROUP  BY div_type_cd 
                                 ")
        sql <- do.call(SqlRender::render, c(list(SqlRawToDeviBy30T_1), renderParams))
        HowManyContainDeviByMappied30T <- DatabaseConnector::querySql(connection, sql)
        sql <- do.call(SqlRender::render, c(list(SqlRawToDeviBy30T_2), renderParams))
        tmp <- DatabaseConnector::querySql(connection, sql)
        names(tmp) <- names(HowManyContainDeviByMappied30T)
        HowManyContainDeviByMappied30T <- rbind(HowManyContainDeviByMappied30T, tmp)
        
        
        ## 60T Mappied
        SqlMappied60T <- c("
                           SELECT Count(*) -- 2
                           FROM   (SELECT x.key_seq, x.div_cd 
                           FROM   (SELECT * 
                           FROM   @NHISNSC_rawdata.@NHIS_60T 
                           WHERE  div_type_cd NOT IN ( '1', '2', '3', '4', '5' )) x, 
                           @NHISNSC_database.SEQ_MASTER y 
                           WHERE  y.source_table = '160' 
                           AND x.key_seq = y.key_seq 
                           AND x.seq_no = y.seq_no) a 
                           JOIN #mapping_table b 
                           ON a.div_cd = b.source_code; 
                           ")
        sql <- do.call(SqlRender::render, c(list(SqlMappied60T), renderParams))
        ConvertedDeviCountByMappied60T <- DatabaseConnector::querySql(connection, sql)
        
        ## 60T Dup Mappied
        SqlDupMappied60T <- c("
                              SELECT Count(*) -- 0
                              FROM   (SELECT x.key_seq, 
                              x.div_cd 
                              FROM   (SELECT * 
                              FROM   @NHISNSC_rawdata.@NHIS_60T 
                              WHERE  div_type_cd IN ( '7', '8' )) x, 
                              @NHISNSC_database.SEQ_MASTER y 
                              WHERE  y.source_table = '160' 
                              AND x.key_seq = y.key_seq 
                              AND x.seq_no = y.seq_no) a 
                              JOIN #duplicated b 
                              ON a.div_cd = b.source_code 
                              ")
        sql <- do.call(SqlRender::render, c(list(SqlDupMappied60T), renderParams))
        ConvertedDeviCountByDupMappied60T <- DatabaseConnector::querySql(connection, sql)
        
        ## 60T UnMappied
        SqlUnMappied <- c("
                          SELECT Count(*) -- 795
                          FROM   (SELECT x.key_seq, 
                          x.div_cd 
                          FROM   (SELECT * 
                          FROM   @NHISNSC_rawdata.@NHIS_60T 
                          WHERE  div_type_cd IN ( '7', '8' )) x, 
                          @NHISNSC_database.SEQ_MASTER y 
                          WHERE  y.source_table = '160' 
                          AND x.key_seq = y.key_seq 
                          AND x.seq_no = y.seq_no) a 
                          WHERE  a.div_cd NOT IN (SELECT source_code 
                          FROM   #duplicated 
                          UNION ALL 
                          SELECT source_code 
                          FROM   #mapping_table); 
                          ")
        sql <- do.call(SqlRender::render, c(list(SqlUnMappied), renderParams))
        ConvertedDeviCountByUnMappied60T <- DatabaseConnector::querySql(connection, sql)
        
        ## 60T Raw
        SqlRawToDeviBy60T_1 <- c("
                                 SELECT div_type_cd, multimappied, Count(*) AS COUNT 
                                 from (select key_seq, seq_no, div_type_cd, count(*) as multimappied
                                 FROM   (SELECT * 
                                 FROM   (SELECT * 
                                 FROM   @NHISNSC_rawdata.@NHIS_60T 
                                 WHERE  div_type_cd NOT IN ( '1', '2', '3', '4', '5', '7', '8' )) x 
                                 JOIN #mapping_table y -- 1:1 mapping 
                                 ON x.div_cd = y.source_code) z 
                                 group by key_seq, seq_no, div_type_cd) a
                                 group by div_type_cd, multimappied
                                 ")
        SqlRawToDeviBy60T_2 <- c("
                                 SELECT div_type_cd, 1 as multimappied, Count(*) AS COUNT
                                 FROM   (SELECT * 
                                 FROM   @NHISNSC_rawdata.@NHIS_60T 
                                 WHERE  div_type_cd IN ( '7', '8' )) x 
                                 GROUP  BY div_type_cd 
                                 ")
        sql <- do.call(SqlRender::render, c(list(SqlRawToDeviBy60T_1), renderParams))
        HowManyContainDeviByMappied60T <- DatabaseConnector::querySql(connection, sql)
        sql <- do.call(SqlRender::render, c(list(SqlRawToDeviBy60T_2), renderParams))
        tmp <- DatabaseConnector::querySql(connection, sql)
        names(tmp) <- names(HowManyContainDeviByMappied60T)
        HowManyContainDeviByMappied60T <- rbind(HowManyContainDeviByMappied60T, tmp)
        
    } ## DeviceTable : 20T join으로 인해 2건 차이남
    
    
    
    
    if(condition_occurrence){
        
        ## Mapping Table
        SqlMapping <- c("
                        IF OBJECT_ID('tempdb..#mapping_table', 'U') IS NOT NULL
                        DROP TABLE #mapping_table;
                        IF OBJECT_ID('tempdb..#mapping_table2', 'U') IS NOT NULL
                        DROP TABLE #mapping_table2;
                        select a.source_code, a.target_concept_id, a.domain_id, REPLACE(a.invalid_reason, '', NULL) as invalid_reason
                        into #mapping_table
                        from @Mapping_database.source_to_concept_map a join @Mapping_database.CONCEPT b on a.target_concept_id=b.concept_id
                        where a.invalid_reason is null and b.invalid_reason is null and a.domain_id='condition';
                        
                        select a.source_code, a.target_concept_id, a.domain_id, REPLACE(a.invalid_reason, '', NULL) as invalid_reason
                        into #mapping_table2
                        from @Mapping_database.source_to_concept_map a join @Mapping_database.CONCEPT b on a.target_concept_id=b.concept_id
                        where a.invalid_reason is null and b.invalid_reason is null;
                        ")
        sql <- do.call(SqlRender::render, c(list(SqlMapping), renderParams))
        DatabaseConnector::executeSql(connection, sql)
        
        
        ## 40T Mappied
        SqlMappied40T <- c("
                           select count(*) from (select a.person_id, sick_sym -- 292,249,453
                           from (select * from @NHISNSC_database.SEQ_MASTER where source_table='140') a,
                           @NHISNSC_rawdata.@NHIS_20T b,
                           @NHISNSC_rawdata.@NHIS_40T c
                           where a.person_id=b.person_id
                           and a.key_seq=b.key_seq
                           and a.key_seq=c.key_seq
                           and a.seq_no=c.seq_no) as m,
                           #mapping_table as n
                           where m.sick_sym=n.source_code;
                           ")
        sql <- do.call(SqlRender::render, c(list(SqlMappied40T), renderParams))
        ConvertedCondiCountByMappied30T <- DatabaseConnector::querySql(connection, sql)
        
        ## 40T UnMappied
        SqlUnMappied40T <- c("
                             select count(*) from (select a.person_id, sick_sym -- 7,176,297
                             from (select * from @NHISNSC_database.SEQ_MASTER where source_table='140') a, 
                             @NHISNSC_rawdata.@NHIS_20T b, 
                             @NHISNSC_rawdata.@NHIS_40T c
                             where a.person_id=b.person_id
                             and a.key_seq=b.key_seq
                             and a.key_seq=c.key_seq
                             and a.seq_no=c.seq_no) as m
                             where m.sick_sym not in (select source_code from #mapping_table2)
                             ")
        sql <- do.call(SqlRender::render, c(list(SqlUnMappied40T), renderParams))
        ConvertedCondiCountByUnMappied40T <- DatabaseConnector::querySql(connection, sql)
        
        ## 40T Raw
        SqlRawToCondiBy40T_1 <- c("
                                  SELECT domain_id, multimappied, Count(*) AS COUNT 
                                  FROM   (SELECT key_seq, seq_no, domain_id, Count(*) AS multimappied 
                                  FROM   (SELECT * 
                                  FROM   @NHISNSC_rawdata.@NHIS_40T a 
                                  JOIN #mapping_table2 b ON a.sick_sym = b.source_code 
                                  WHERE  b.domain_id = 'condition') c -- 1:n mappied  292,250,891   
                                  GROUP  BY key_seq, seq_no, domain_id) d 
                                  GROUP  BY domain_id, multimappied  
                                  ")
        SqlRawToCondiBy40T_2 <- c("
                                  SELECT 'Unclassified' as domain_id, 1 as multimappied, Count(*) AS COUNT
                                  FROM   @NHISNSC_rawdata.@NHIS_40T 
                                  WHERE  sick_sym NOT IN (SELECT source_code FROM #mapping_table2) 
                                  ")
        sql <- do.call(SqlRender::render, c(list(SqlRawToCondiBy40T_1), renderParams))
        HowManyContainCondiByMappied40T <- DatabaseConnector::querySql(connection, sql)
        sql <- do.call(SqlRender::render, c(list(SqlRawToCondiBy40T_2), renderParams))
        tmp <- DatabaseConnector::querySql(connection, sql)
        names(tmp) <- names(HowManyContainCondiByMappied40T)
        HowManyContainCondiByMappied40T <- rbind(HowManyContainCondiByMappied40T, tmp)
        
        DQresults$condition_occurrence <- list(
            Mapped_40T = ConvertedCondiCountByMappied30T,
            Unmapped_40T = ConvertedCondiCountByUnMappied40T,
            Raw_40T = HowManyContainCondiByMappied40T
        )
        
    } ## ConditionOccurrenceTable : 20T join으로 인해 3건 차이남, 기간 제거 개수 확인요망
    
    
    
    
    if(measurement){
        
        ## Mapping Table
        SqlMapping <- c("
                        IF OBJECT_ID('tempdb..#measurement_mapping', 'U') IS NOT NULL
                        DROP TABLE #measurement_mapping;
                        CREATE TABLE #measurement_mapping
                        (
                        meas_type					varchar(50)					NULL , 
                        id_value					varchar(50)					NULL ,
                        answer						bigint						NULL ,
                        measurement_concept_id		bigint						NULL ,
                        measurement_type_concept_id	bigint						NULL ,
                        measurement_unit_concept_id	bigint						NULL ,
                        value_as_concept_id			bigint						NULL ,
                        value_as_number				float						NULL 
                        )
                        ;
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('HEIGHT',			'01',	0,	3036277,	44818701,	4122378,	NULL,		NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('WEIGHT',			'02',	0,	3025315,	44818701,	4122383,	NULL,		NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('WAIST',				'03',	0,	3016258,	44818701,	4122378,	NULL,		NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('BP_HIGH',			'04',	0,	3028737,	44818701,	4118323,	NULL,		NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('BP_LWST',			'05',	0,	3012888,	44818701,	4118323,	NULL,		NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('BLDS',				'06',	0,	46235168,	44818702,	4121396,	NULL,		NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('TOT_CHOLE',			'07',	0,	3027114,	44818702,	4121396,	NULL,		NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('TRIGLYCERIDE',		'08',	0,	3022038,	44818702,	4121396,	NULL,		NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('HDL_CHOLE',			'09',	0,	3023752,	44818702,	4121396,	NULL,		NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('LDL_CHOLE',			'10',	0,	3028437,	44818702,	4121396,	NULL,		NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('HMG',				'11',	0,	3000963,	44818702,	4121395,	NULL,		NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('GLY_CD',			'12',	1,	3009261,	44818702,	NULL,		9189,		NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('GLY_CD',			'12',	2,	3009261,	44818702,	NULL,		4127785,	NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('GLY_CD',			'12',	3,	3009261,	44818702,	NULL,		4123508,	NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('GLY_CD',			'12',	4,	3009261,	44818702,	NULL,		4126673,	NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('GLY_CD',			'12',	5,	3009261,	44818702,	NULL,		4125547,	NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('GLY_CD',			'12',	6,	3009261,	44818702,	NULL,		4126674,	NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('OLIG_OCCU_CD',		'13',	1,	437038,		44818702,	NULL,		9189,		NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('OLIG_OCCU_CD',		'13',	2,	437038,		44818702,	NULL,		4127785,	NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('OLIG_OCCU_CD',		'13',	3,	437038,		44818702,	NULL,		4123508,	NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('OLIG_OCCU_CD',		'13',	4,	437038,		44818702,	NULL,		4126673,	NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('OLIG_OCCU_CD',		'13',	5,	437038,		44818702,	NULL,		4125547,	NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('OLIG_OCCU_CD',		'13',	6,	437038,		44818702,	NULL,		4126674,	NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('OLIG_PH',			'14',	0,	3015736,	44818702,	8482,		NULL,		NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('OLIG_PROTE_CD',		'15',	1,	3014051,	44818702,	NULL,		9189,		NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('OLIG_PROTE_CD',		'15',	2,	3014051,	44818702,	NULL,		4127785,	NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('OLIG_PROTE_CD',		'15',	3,	3014051,	44818702,	NULL,		4123508,	NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('OLIG_PROTE_CD',		'15',	4,	3014051,	44818702,	NULL,		4126673,	NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('OLIG_PROTE_CD',		'15',	5,	3014051,	44818702,	NULL,		4125547,	NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('OLIG_PROTE_CD',		'15',	6,	3014051,	44818702,	NULL,		4126674,	NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('CREATININE',		'16',	0,	2212294,	44818702,	4121396,	NULL,		NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('SGOT_AST',			'17',	0,	2212597,	44818702,	4118000,	NULL,		NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('SGPT_ALT',			'18',	0,	2212598,	44818702,	4118000,	NULL,		NULL)
                        insert into #measurement_mapping (meas_type, id_value, answer, measurement_concept_id, measurement_type_concept_id, measurement_unit_concept_id, value_as_concept_id, value_as_number) values ('GAMMA_GTP',			'19',	0,	4289475,	44818702,	4118000,	NULL,		NULL)
                        ")
        sql <- do.call(SqlRender::render, c(list(SqlMapping), renderParams))
        DatabaseConnector::executeSql(connection, sql)
        
        
        ## GJ 수치형
        SqlMappiedGJ_num <- c("
                              SELECT Count(*) as COUNT -- 29,145,003
                              FROM   (SELECT a.meas_type, meas_value, hchk_year, person_id 
                              FROM   @NHISNSC_rawdata.@GJ_vertical a, -- left join 75,717,081, 원래 75,298,684 -> 1:n mappig
                              #measurement_mapping b 
                              where  Isnull(a.meas_type, '') = Isnull(b.meas_type, '') 
                              AND Isnull(a.meas_value, '0') >= Isnull(Cast(b.answer AS CHAR), '0')) c, --  33,858,848 -> 1:1 mapping
                              @NHISNSC_rawdata.@NHIS_GJ d 
                              where  c.person_id = Cast(d.person_id AS CHAR) 
                              AND c.hchk_year = d.hchk_year 
                              AND c.meas_value != '' 
                              AND Substring(c.meas_type, 1, 30) IN ( 
                              'HEIGHT', 'WEIGHT', 'WAIST', 'BP_HIGH', 'BP_LWST', 'BLDS', 'TOT_CHOLE', 'TRIGLYCERIDE', 
                              'HDL_CHOLE', 'LDL_CHOLE', 'HMG', 'OLIG_PH', 'CREATININE', 'SGOT_AST', 'SGPT_ALT', 'GAMMA_GTP' ) 
                              ")
        sql <- do.call(SqlRender::render, c(list(SqlMappiedGJ_num), renderParams))
        ConvertedMeasuCountByMappiedGJ_num <- DatabaseConnector::querySql(connection, sql)
        
        ## GJ 코드형
        SqlMappiedGJ_code <- c("
                               SELECT Count(*) as COUNT -- 4,295,448
                               FROM   (SELECT a.meas_type, meas_value, hchk_year, person_id 
                               FROM   @NHISNSC_rawdata.@GJ_vertical a, -- left join 75,298,684, 원래 75,298,684 -> 1:1 mappig
                               #measurement_mapping b 
                               where  Isnull(a.meas_type, '') = Isnull(b.meas_type, '') 
                               AND Isnull(a.meas_value, '0') = Isnull(Cast(b.answer AS CHAR), '0')) c, --  -> 1:1 mapping
                               @NHISNSC_rawdata.@NHIS_GJ d 
                               where   c.person_id = Cast(d.person_id AS CHAR) 
                               AND c.hchk_year = d.hchk_year 
                               AND c.meas_value != '' 
                               AND Substring(c.meas_type, 1, 30) IN ( 'GLY_CD', 'OLIG_OCCU_CD', 'OLIG_PROTE_CD' ) 
                               ")
        sql <- do.call(SqlRender::render, c(list(SqlMappiedGJ_code), renderParams))
        ConvertedMeasuCountByMappiedGJ_code <- DatabaseConnector::querySql(connection, sql)
        
        DQresults$measurement <- list(
            Mapped_GJ_numeric = ConvertedMeasuCountByMappiedGJ_num,
            Mapped_GJ_code = ConvertedMeasuCountByMappiedGJ_code
        )
        
    }
    
    ## 결과 요약 출력
    cat("\n========== DQ Evaluation Results ==========\n")
    for (nm in names(DQresults)) {
        cat("\n--- ", nm, " ---\n", sep = "")
        for (item in names(DQresults[[nm]])) {
            x <- DQresults[[nm]][[item]]
            if (is.data.frame(x) && nrow(x) >= 1 && ncol(x) >= 1) {
                cntcol <- which(tolower(names(x)) %in% c("count", "cnt"))[1]
                if (!is.na(cntcol) && nrow(x) == 1) {
                    cat("  ", item, ": ", x[[cntcol]], "\n", sep = "")
                } else if (nrow(x) <= 10) {
                    cat("  ", item, ":\n", sep = "")
                    print(x)
                } else {
                    cat("  ", item, ": (", nrow(x), " rows)\n", sep = "")
                    print(head(x, 5))
                }
            } else {
                print(x)
            }
        }
    }
    cat("\n============================================\n")
    
    return(invisible(DQresults))
}
