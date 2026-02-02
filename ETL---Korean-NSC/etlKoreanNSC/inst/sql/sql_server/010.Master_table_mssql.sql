/**************************************
 --encoding : UTF-8
 --Author: SW Lee, JM Park
 --Date: 2018.08.21
 
 @NHISNSC_raw : DB containing NHIS National Sample cohort DB
 @NHISNSC_database : DB for NHIS-NSC in CDM format
 @NHID_JK: JK table in NHIS NSC
 @NHID_20: 20 table in NHIS NSC
 @NHID_30: 30 table in NHIS NSC
 @NHID_40: 40 table in NHIS NSC
 @NHID_60: 60 table in NHIS NSC
 @NHID_GJ: GJ table in NHIS NSC
 --Description: Among T1 tables of sample cohort, keep primary keys of 30T, 40T, 60T, GJ, JK table and create table which has unique serial number.
				The serial number is used as primary key of condition, drug, procedure and device tables, and the serial number of GJ table will be used as priomary key of visit_occurrence table.
			   the serial number of JK table will be used as priomary key of observation table.
			   Those keys is created for tracking sample cogort DB in converted CDM DB
 --Generating Table: SEQ_MASTER
***************************************/

/**************************************
 1. Create table
    : serial number(PK), source table, person_id, primary keys of 30T, 40T, 60T, GJ, JK tables
***************************************/  
CREATE TABLE nhisnsc2013cdm.dbo.SEQ_MASTER (
	master_seq		BIGINT	identity(1, 1) PRIMARY KEY,
	source_table	CHAR(3)	NOT NULL, -- 30T = 130, 40T = 140, 60T = 160 GJ ='GJT', JK = 'JKT'
	person_id		INT	NOT NULL, 
	key_seq			BIGINT	NULL, -- 30T, 40T, 60T
	seq_no			NUMERIC(4)	NULL, -- 30T, 40T, 60T
	hchk_year		CHAR(4)	NULL, -- GJ
	stnd_y			CHAR(4) NULL, -- JK 
)


/**************************************
 2. Insert data of 30T
    : serial number is starting from 3000000001
***************************************/
-- 1) Reset the serial number
DBCC CHECKIDENT('nhisnsc2013cdm.dbo.seq_master', RESEED, 3000000000);

-- 2) Insert data
INSERT INTO nhisnsc2013cdm.dbo.SEQ_MASTER
	(source_table, person_id, key_seq, seq_no)
SELECT '130', b.person_id, a.key_seq, a.seq_no
FROM nhisnsc2013original.dbo.NHID_30 a, nhisnsc2013original.dbo.NHID_20 b
WHERE a.key_seq=b.key_seq
;

/**************************************
 3. Insert data of 40T
    : serial number is starting from 4000000001
***************************************/
-- 1) Reset the serial number
DBCC CHECKIDENT('nhisnsc2013cdm.dbo.seq_master', RESEED, 4000000000);

-- 2) Insert data
INSERT INTO nhisnsc2013cdm.dbo.SEQ_MASTER
	(source_table, person_id, key_seq, seq_no)
SELECT '140', b.person_id, a.key_seq, a.seq_no
FROM nhisnsc2013original.dbo.NHID_40 a, nhisnsc2013original.dbo.NHID_20 b
WHERE a.key_seq=b.key_seq
;

/**************************************
 4. Insert data of 60T
    : serial number is starting from 6000000001
***************************************/
-- 1) Reset the serial number
DBCC CHECKIDENT('nhisnsc2013cdm.dbo.seq_master', RESEED, 6000000000);

-- 2) Insert data
INSERT INTO nhisnsc2013cdm.dbo.SEQ_MASTER
	(source_table, person_id, key_seq, seq_no)
SELECT '160', b.person_id, a.key_seq, a.seq_no
FROM nhisnsc2013original.dbo.NHID_60 a, nhisnsc2013original.dbo.NHID_20 b
WHERE a.key_seq=b.key_seq
;

/**************************************
 5. Insert data of GJ table
    : serial number is starting from 800000000001
	: visit_occurrence_id is consisted with 12 numbers, match the numbers
***************************************/
-- 1) Reset the serial number
DBCC CHECKIDENT('nhisnsc2013cdm.dbo.seq_master', RESEED, 800000000000);

-- 2) Insert data
INSERT INTO nhisnsc2013cdm.dbo.SEQ_MASTER
	(source_table, person_id, hchk_year)
SELECT 'GJT', person_id, hchk_year
FROM nhisnsc2013original.dbo.NHID_GJ
GROUP BY hchk_year, person_id
;
/**************************************
 6. Insert data of JK table
	: serial number is starting from 900000000001
**************************************/
-- 1) Reset the serial number
DBCC CHECKIDENT('nhisnsc2013cdm.dbo.seq_master', RESEED, 900000000000);

-- 2) Insert data
INSERT INTO nhisnsc2013cdm.dbo.SEQ_MASTER
	(source_table, person_id, stnd_y)
SELECT 'JKT', person_id, STND_Y
FROM nhisnsc2013original.dbo.NHID_JK
GROUP BY STND_Y, person_id;
;