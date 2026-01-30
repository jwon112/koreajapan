/*********************************************************************************
# Copyright 2014 Observational Health Data Sciences and Informatics
#
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
********************************************************************************/

/************************
 ####### #     # ####### ######      #####  ######  #     #           ####### 
 #     # ##   ## #     # #     #    #     # #     # ##   ##    #    # #       
 #     # # # # # #     # #     #    #       #     # # # # #    #    # #       
 #     # #  #  # #     # ######     #       #     # #  #  #    #    # ######  
 #     # #     # #     # #          #       #     # #     #    #    #       # 
 #     # #     # #     # #          #     # #     # #     #     #  #  #     # 
 ####### #     # ####### #           #####  ######  #     #      ##    #####  
                                                                              
Script to load the common data model, version 5.0 vocabulary tables for SQL Server database
Notes
1) There is no data file load for the SOURCE_TO_CONCEPT_MAP table because that table is deprecated in CDM version 5.0
2) This script assumes the CDM version 5 vocabulary zip file has been unzipped into the "@vocaFolderulary" directory. 
3) If you unzipped your CDM version 5 vocabulary files into a different directory then replace all file paths below, with your directory path.
4) Run this SQL query script in the database where you created your CDM Version 5 tables
last revised: 26 Nov 2014
author:  Lee Evans
*************************/
USE @Mapping_database;

-- 1. CONCEPT
TRUNCATE TABLE CONCEPT;
BULK INSERT CONCEPT FROM '@vocaFolder\CONCEPT.csv' 
WITH (FIRSTROW = 2, FIELDTERMINATOR = '\t', ROWTERMINATOR = '0x0a', CODEPAGE = '65001', TABLOCK);

-- 2. CONCEPT_SYNONYM
TRUNCATE TABLE CONCEPT_SYNONYM;
BULK INSERT CONCEPT_SYNONYM FROM '@vocaFolder\CONCEPT_SYNONYM.csv' 
WITH (FIRSTROW = 2, FIELDTERMINATOR = '\t', ROWTERMINATOR = '0x0a', CODEPAGE = '65001', TABLOCK);

-- 3. CONCEPT_RELATIONSHIP
TRUNCATE TABLE CONCEPT_RELATIONSHIP;
BULK INSERT CONCEPT_RELATIONSHIP FROM '@vocaFolder\CONCEPT_RELATIONSHIP.csv' 
WITH (FIRSTROW = 2, FIELDTERMINATOR = '\t', ROWTERMINATOR = '0x0a', CODEPAGE = '65001', TABLOCK);

-- 4. CONCEPT_ANCESTOR
TRUNCATE TABLE CONCEPT_ANCESTOR;
BULK INSERT CONCEPT_ANCESTOR FROM '@vocaFolder\CONCEPT_ANCESTOR.csv' 
WITH (FIRSTROW = 2, FIELDTERMINATOR = '\t', ROWTERMINATOR = '0x0a', CODEPAGE = '65001', TABLOCK);

-- 5. DRUG_STRENGTH
TRUNCATE TABLE DRUG_STRENGTH;
BULK INSERT DRUG_STRENGTH FROM '@vocaFolder\DRUG_STRENGTH.csv' 
WITH (FIRSTROW = 2, FIELDTERMINATOR = '\t', ROWTERMINATOR = '0x0a', CODEPAGE = '65001', TABLOCK);

-- 6. VOCABULARY
TRUNCATE TABLE VOCABULARY;
BULK INSERT VOCABULARY FROM '@vocaFolder\VOCABULARY.csv' 
WITH (FIRSTROW = 2, FIELDTERMINATOR = '\t', ROWTERMINATOR = '0x0a', CODEPAGE = '65001', TABLOCK);

-- 7. DOMAIN
TRUNCATE TABLE DOMAIN;
BULK INSERT DOMAIN FROM '@vocaFolder\DOMAIN.csv' 
WITH (FIRSTROW = 2, FIELDTERMINATOR = '\t', ROWTERMINATOR = '0x0a', CODEPAGE = '65001', TABLOCK);

-- 8. CONCEPT_CLASS
TRUNCATE TABLE CONCEPT_CLASS;
BULK INSERT CONCEPT_CLASS FROM '@vocaFolder\CONCEPT_CLASS.csv' 
WITH (FIRSTROW = 2, FIELDTERMINATOR = '\t', ROWTERMINATOR = '0x0a', CODEPAGE = '65001', TABLOCK);

-- 9. RELATIONSHIP
TRUNCATE TABLE RELATIONSHIP;
BULK INSERT RELATIONSHIP FROM '@vocaFolder\RELATIONSHIP.csv' 
WITH (FIRSTROW = 2, FIELDTERMINATOR = '\t', ROWTERMINATOR = '0x0a', CODEPAGE = '65001', TABLOCK);