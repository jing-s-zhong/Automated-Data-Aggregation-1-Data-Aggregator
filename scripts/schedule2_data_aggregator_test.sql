!set variable_substitution=true;
use database &{db_name};
use schema &{sc_name};
--
-------------------------------------------------------
-- Create task management tables
-------------------------------------------------------
--
-- CREATE TABLE DATA_AGGREGATION_TARGETS;
-- CREATE TABLE DATA_AGGREGATION_SOURCES;
-- CREATE FUNCTION DATA_PATTERN(ARRAY);
-- CREATE FUNCTION COLUMN_MAP(ARRAY);
-- CREATE FUNCTION REVENUE_SHARE(VARIANT, VARCHAR, FLOAT);
-- CREATE FUNCTION REVENUE_SHARE(VARIANT, VARCHAR);
-- CREATE PROCEDURE DATA_AGGREGATOR(STRING, STRING, BOOLEAN);
-- CREATE PROCEDURE DATA_AGGREGATOR(STRING, BOOLEAN);
--
-------------------------------------------------------
-- Test REVENUE_SHARE
-------------------------------------------------------
--
-------------------------------------------------------
-- Test 2 types of the DATA_PATTERN function calls, type 1 is used by setup automation
-------------------------------------------------------
SELECT DATA_PATTERN(PARSE_JSON('[1,1,0,0,1,1,0,0,0,1,1,1,1,0,0,0,0,0,0,0,0]')) PATTERN_1,
   DATA_PATTERN(PARSE_JSON('{"pattern_columns":[
      "DATA_PATTERN",
      "DATA_DATE",
      "DATA_HOUR",
      "DATA_TIME",
      "BUSINESS_UNIT",
      "BUSINESS_UNIT_DETAIL",
      "PROPERTY_TYPE",
      "PROPERTY_DETAIL",
      "PLACEMENT",
      "PROVIDER",
      "NETWORK",
      "ACCOUNT",
      "PARTNER_TAG",
      "TYPE_TAG",
      "CHANNEL",
      "PRODUCT",
      "MARKET",
      "COUNTRY",
      "DEVICE",
      "BIDDER",
      "CONTRACT"
  ], "groupby_columns":[
      "DATA_PATTERN",
      "DATA_DATE",
      "BUSINESS_UNIT",
      "BUSINESS_UNIT_DETAIL",
      "PROVIDER",
      "NETWORK",
      "ACCOUNT",
      "PARTNER_TAG",
  ]}')) PATTERN_2;
-------------------------------------------------------
-- Create two dummy aggreagtion data sources
-------------------------------------------------------
-- 
-- Create dummay aggregation data source 1
-- 
-- DROP TABLE SOURCE_DATA_1;
CREATE OR REPLACE TRANSIENT TABLE SOURCE_DATA_1
AS
SELECT 0 DATA_PT,
	DATEADD(D, -UNIFORM(1, 60, RANDOM(1)), CURRENT_DATE())::DATE DATA_TS, 
    1 DATA_I1,
    UNIFORM(0, 15, RANDOM(11)) DATA_I2,
    NULLIF(UNIFORM(0, 15, RANDOM(111)),0) DATA_I3,
    RANDSTR(UNIFORM(1, 10, RANDOM()), RANDOM()) DATA_A1,
    RANDSTR(ABS(RANDOM()) % 10, RANDOM()) DATA_A2,
    NULLIF(RANDSTR(UNIFORM(0, 10, RANDOM()), RANDOM()),'') DATA_A3,
    UNIFORM(0, 1000, RANDOM(10)) VALUE_I1,
    UNIFORM(0, 1500, RANDOM(15))/10 VALUE_D1
FROM TABLE(GENERATOR(ROWCOUNT => 1000)) V 
ORDER BY 1;
--
--UPDATE SOURCE_DATA_1
--SET DATA_PT = _CONTROL_LOGIC.DATA_PATTERN(
--    DATA_PT,
--    DATA_TS, 
--    DATA_I1,
--    DATA_I2,
--    DATA_I3,
--    DATA_A1,
--    DATA_A2,
--    DATA_A3
--);
-- 
-- Create dummay aggregation data source 2
-- 
-- DROP TABLE SOURCE_DATA_1;
CREATE OR REPLACE TRANSIENT TABLE SOURCE_DATA_2
AS
SELECT 0 DATA_PT,
	DATEADD(D, -UNIFORM(1, 60, RANDOM(2)), CURRENT_DATE())::DATE DATA_TS, 
    1 DATA_I1,
    UNIFORM(0, 15, RANDOM(22)) DATA_I2,
    NULLIF(UNIFORM(0, 15, RANDOM(222)),0) DATA_I3,
    RANDSTR(UNIFORM(1, 10, RANDOM()), RANDOM()) DATA_A1,
    RANDSTR(ABS(RANDOM()) % 10, RANDOM()) DATA_A2,
    NULLIF(RANDSTR(UNIFORM(0, 10, RANDOM()), RANDOM()),'') DATA_A3,
    UNIFORM(0, 1000, RANDOM(10)) VALUE_I1,
    UNIFORM(0, 1500, RANDOM(15))/10 VALUE_D1
FROM TABLE(GENERATOR(ROWCOUNT => 1000)) V 
ORDER BY 1;
--
--UPDATE SOURCE_DATA_2
--SET DATA_PT = _CONTROL_LOGIC.DATA_PATTERN(
--    DATA_PT,
--    DATA_TS, 
--    DATA_I1,
--    DATA_I2,
--    DATA_I3,
--    DATA_A1,
--    DATA_A2,
--    DATA_A3
--);
-------------------------------------------------------
-- Create two dummy aggreagtion data targets 
-------------------------------------------------------
--
-- Create dummay aggregation data target 1
-- 
CREATE OR REPLACE TRANSIENT TABLE TARGET_DATA_1 (
	"DATA_PT" 								NUMBER     NOT NULL, 
	"DATA_TS" 								DATE       NOT NULL, 
	"DATA_I1" 								NUMBER     NOT NULL, 
	"DATA_I2"	 							NUMBER, 
	"DATA_I3"	 							NUMBER, 
	"DATA_A1"	 							VARCHAR, 
	"DATA_A2"	 							VARCHAR, 
	"DATA_A3"	 							VARCHAR, 
	"VALUE_I1" 								NUMBER, 
	"VALUE_D1" 								FLOAT
); 
--
-- Register the tagegt table 1
-- 
-- DELETE FROM DATA_AGGREGATION_TARGETS WHERE TARGET_TABLE = 'TARGET_DATA_1';
INSERT INTO DATA_AGGREGATION_TARGETS (
	TARGET_LABEL
	,TARGET_TABLE
	,BATCH_CONTROL_COLUMN
	,BATCH_CONTROL_SIZE
	,BATCH_CONTROL_NEXT
	,BATCH_PROCESSED
	,BATCH_PROCESSING
	,BATCH_MICROCHUNK_CURRENT
	,BATCH_SCHEDULE_TYPE
	,BATCH_SCHEDULE_LAST
	,PATTERN_COLUMNS
	,GROUPBY_COLUMNS
	,GROUPBY_PATTERN
	,GROUPBY_FLEXIBLE
	,AGGREGATE_COLUMNS
	,AGGREGATE_FUNCTIONS
	,DEFAULT_PROCEDURE
	)
SELECT 'Test: Dummay aggregation target 1'
	,$1
	,$2
	,$3
	,$4
	,DATE_TRUNC('DAY', CURRENT_DATE ()) - 31
	,NULL
	,NULL
	,$5
	,NULL
	,PARSE_JSON($6)
	,PARSE_JSON($7)
	,DATA_PATTERN(PARSE_JSON($8))
	,True
	,PARSE_JSON($9)
	,PARSE_JSON($10)
	,NULL
FROM
VALUES (
	'TARGET_DATA_1'
	,'DATA_TS'
	, 1440
	,'DATEADD(MINUTE, :2, :1)'
	,'DAILY'
	-- all group-by columns in source data
	,'["DATA_PT",
		"DATA_TS", 
		"DATA_I1",
		"DATA_I2", 
		"DATA_I3", 
		"DATA_A1", 
		"DATA_A2", 
		"DATA_A3"
      ]'
	-- group-by columns of target data and which source column is the match
	,'["DATA_PT:DATA_PT", 
		"DATA_TS:DATA_TS",
		"DATA_I1:DATA_I1", 
		"DATA_I2:DATA_I2", 
		"DATA_I3:DATA_I3", 
		"DATA_A1:DATA_A1", 
		"DATA_A2:DATA_A2", 
		"DATA_A3:DATA_A3"
       ]'
	-- indicators of which group-by column are needed in target table
	,'[1,1,1,1,1,1,1,1]'
	-- aggregate columns of target data and which aggregating column is the match
	,'["VALUE_I1:VALUE_I1","VALUE_D1:VALUE_D1"]'
	-- what aggregation function will be used for every aggregation column
	,'["SUM(?)","SUM(?)"]'
	);
--
-- Register the source data table
-- 
-- DELETE FROM DATA_AGGREGATION_SOURCES WHERE TARGET_TABLE = 'TARGET_DATA_1' AND SOURCE_TABLE = 'SOURCE_DATA_1';
INSERT INTO DATA_AGGREGATION_SOURCES (
	SOURCE_LABEL
	,TARGET_TABLE
	,SOURCE_TABLE
	,SOURCE_ENABLED
	,PATTERN_DEFAULT
	,PATTERN_FLEXIBLE
	,DATA_AVAILABLETIME
	,DATA_CHECKSCHEDULE
	,TRANSFORMATION
	)
SELECT 'Test: Dummay aggregation target 1 source 1'
    ,$1
	,$2
	,true
	,0
	,False
	,DATE_TRUNC('DAY', CURRENT_DATE()) -2
	,NULL
	,$3
FROM
VALUES (
	'TARGET_DATA_1'
	,'SOURCE_DATA_1'
	,'SOURCE_DATA_1'
	);
--
-- Register the source data table
-- 
-- DELETE FROM DATA_AGGREGATION_SOURCES WHERE TARGET_TABLE = 'TARGET_DATA_1' AND SOURCE_TABLE = 'SOURCE_DATA_2';
INSERT INTO DATA_AGGREGATION_SOURCES (
	SOURCE_LABEL
	,TARGET_TABLE
	,SOURCE_TABLE
	,SOURCE_ENABLED
	,PATTERN_DEFAULT
	,PATTERN_FLEXIBLE
	,DATA_AVAILABLETIME
	,DATA_CHECKSCHEDULE
	,TRANSFORMATION
	)
SELECT 'Test: Dummay aggregation target 1 source 2'
    ,$1
	,$2
	,true
	,0
	,False
	,DATE_TRUNC('DAY', CURRENT_DATE()) -3
	,NULL
	,$3
FROM
VALUES (
	'TARGET_DATA_1'
	,'SOURCE_DATA_2'
	,'SOURCE_DATA_2'
	);
--
-- Populate summary data
--
CALL DATA_AGGREGATOR('TARGET_DATA_1', '2020-01-07', 1); 
--
--
--
CALL DATA_AGGREGATOR('TARGET_DATA_1', 0); 
--
-- Query target registrtion
--
--
-- Create dummay aggregation data target 2
-- 
CREATE OR REPLACE TRANSIENT TABLE TARGET_DATA_2 (
	"DATA_TS" 								DATE       NOT NULL, 
	"DATA_I1" 								NUMBER     NOT NULL, 
	"DATA_I2"	 							NUMBER, 
	"DATA_I3"	 							NUMBER, 
	"DATA_A1"	 							VARCHAR, 
	"DATA_A2"	 							VARCHAR, 
	"DATA_A3"	 							VARCHAR, 
	"VALUE_I1" 								NUMBER, 
	"VALUE_D1" 								FLOAT
); 
--
-- Register the tagegt table 1
-- 
-- DELETE FROM DATA_AGGREGATION_TARGETS WHERE TARGET_TABLE = 'TARGET_DATA_1';
INSERT INTO DATA_AGGREGATION_TARGETS (
	TARGET_LABEL
	,TARGET_TABLE
	,BATCH_CONTROL_COLUMN
	,BATCH_CONTROL_SIZE
	,BATCH_CONTROL_NEXT
	,BATCH_PROCESSED
	,BATCH_PROCESSING
	,BATCH_MICROCHUNK_CURRENT
	,BATCH_SCHEDULE_TYPE
	,BATCH_SCHEDULE_LAST
	,PATTERN_COLUMNS
	,GROUPBY_COLUMNS
	,GROUPBY_PATTERN
	,GROUPBY_FLEXIBLE
	,AGGREGATE_COLUMNS
	,AGGREGATE_FUNCTIONS
	,DEFAULT_PROCEDURE
	)
SELECT 'Test: Dummay aggregation target 2'
	,$1
	,$2
	,$3
	,$4
	,DATE_TRUNC('DAY', CURRENT_DATE ()) - 31
	,NULL
	,NULL
	,$5
	,NULL
	,PARSE_JSON($6)
	,PARSE_JSON($7)
	,DATA_PATTERN(PARSE_JSON($8))
	,True
	,PARSE_JSON($9)
	,PARSE_JSON($10)
	,NULL
FROM
VALUES (
	'TARGET_DATA_2'
	,'DATA_TS'
	, 1440
	,'DATEADD(MINUTE, :2, :1)'
	,'DAILY'
	-- all group-by columns in source data
	,'["DATA_PT",
		"DATA_TS", 
		"DATA_I1",
		"DATA_I2", 
		"DATA_I3", 
		"DATA_A1", 
		"DATA_A2", 
		"DATA_A3"
      ]'
	-- group-by columns of target data and which source column is the match
	,'["DATA_TS:DATA_TS",
		"DATA_I1:DATA_I1", 
		"DATA_I2:DATA_I2", 
		"DATA_I3:DATA_I3", 
		"DATA_A1:DATA_A1", 
		"DATA_A2:DATA_A2", 
		"DATA_A3:DATA_A3"
       ]'
	-- indicators of which group-by column are needed in target table
	,'[0,1,1,1,1,1,1,1]'
	-- aggregate columns of target data and which aggregating column is the match
	,'["VALUE_I1:VALUE_I1","VALUE_D1:VALUE_D1"]'
	-- what aggregation function will be used for every aggregation column
	,'["SUM(?)","SUM(?)"]'
	);
--
-- Register the source data table
-- 
-- DELETE FROM DATA_AGGREGATION_SOURCES WHERE TARGET_TABLE = 'TARGET_DATA_1' AND SOURCE_TABLE = 'SOURCE_DATA_1';
INSERT INTO DATA_AGGREGATION_SOURCES (
	SOURCE_LABEL
	,TARGET_TABLE
	,SOURCE_TABLE
	,SOURCE_ENABLED
	,PATTERN_DEFAULT
	,PATTERN_FLEXIBLE
	,DATA_AVAILABLETIME
	,DATA_CHECKSCHEDULE
	,TRANSFORMATION
	)
SELECT 'Test: Dummay aggregation target 2 source 1'
    ,$1
	,$2
	,true
	,0
	,False
	,DATE_TRUNC('DAY', CURRENT_DATE()) -2
	,NULL
	,$3
FROM
VALUES (
	'TARGET_DATA_2'
	,'SOURCE_DATA_1'
	,'SOURCE_DATA_1'
	);
--
-- Register the source data table
-- 
-- DELETE FROM DATA_AGGREGATION_SOURCES WHERE TARGET_TABLE = 'TARGET_DATA_1' AND SOURCE_TABLE = 'SOURCE_DATA_2';
INSERT INTO DATA_AGGREGATION_SOURCES (
	SOURCE_LABEL
	,TARGET_TABLE
	,SOURCE_TABLE
	,SOURCE_ENABLED
	,PATTERN_DEFAULT
	,PATTERN_FLEXIBLE
	,DATA_AVAILABLETIME
	,DATA_CHECKSCHEDULE
	,TRANSFORMATION
	)
SELECT 'Test: Dummay aggregation target 2 source 2'
    ,$1
	,$2
	,true
	,0
	,False
	,DATE_TRUNC('DAY', CURRENT_DATE()) -2
	,NULL
	,$3
FROM
VALUES (
	'TARGET_DATA_2'
	,'SOURCE_DATA_2'
	,'SOURCE_DATA_2'
	);
--
-- Populate summary data
--
CALL DATA_AGGREGATOR('TARGET_DATA_2', '2020-01-07', 1); 
--
--
--
CALL DATA_AGGREGATOR('TARGET_DATA_2', 0); 
--
-- Query source registrtion
--
--
-- Query aggregation data source
--
--
-- Check the data for test date
--
select data_ts, count(*) cnt
from "BI_TEST"."_TEST_"."TARGET_DATA_1"
group by 1
order by 1 desc
;
select data_ts, count(*) cnt
from "BI_TEST"."_TEST_"."TARGET_DATA_2"
group by 1
order by 1 desc
;
