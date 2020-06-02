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
-- Create dummy aggregation data source 1
--
-- DROP TABLE _TEST_DATA_SOURCE_1;
CREATE OR REPLACE TRANSIENT TABLE _TEST_DATA_SOURCE_1
AS
SELECT 0::NUMBER DATA_PT,
	DATEADD(MINUTE, -UNIFORM(1, 50000, RANDOM(1)), CURRENT_TIMESTAMP(0))::TIMESTAMP_NTZ DATA_TS,
    1::NUMBER DATA_I1,
    UNIFORM(0, 15, RANDOM(11))::NUMBER DATA_I2,
    NULLIF(UNIFORM(0, 15, RANDOM(111)),0)::NUMBER DATA_I3,
    RANDSTR(UNIFORM(1, 10, RANDOM()), RANDOM())::VARCHAR DATA_A1,
    RANDSTR(ABS(RANDOM()) % 10, RANDOM())::VARCHAR  DATA_A2,
    NULLIF(RANDSTR(UNIFORM(0, 10, RANDOM()), RANDOM()),'')::VARCHAR  DATA_A3,
    UNIFORM(0, 50, RANDOM(10))::NUMBER VALUE_I1,
    UNIFORM(0, 1500, RANDOM(15))/10::FLOAT VALUE_D1
FROM TABLE(GENERATOR(ROWCOUNT => 50000)) V
ORDER BY 1;
--
UPDATE _TEST_DATA_SOURCE_1
SET DATA_PT = DATA_PATTERN(ARRAY_CONSTRUCT(
    1,
    '_TEST_DATA_SOURCE_1',
    DATE(DATA_TS),
    DATE_PART(HOUR, DATA_TS),
    DATA_TS,
    DATA_I1,
    DATA_I2,
    DATA_I3,
    DATA_A1,
    DATA_A2,
    DATA_A3
));
--
-- Create dummy aggregation data source 2
--
-- DROP TABLE _TEST_DATA_SOURCE_2;
CREATE OR REPLACE TRANSIENT TABLE _TEST_DATA_SOURCE_2
AS
SELECT 0::NUMBER DATA_PT,
	DATEADD(MINUTE, -UNIFORM(1, 50000, RANDOM(2)), CURRENT_TIMESTAMP(0))::TIMESTAMP_NTZ DATA_TS,
    2::NUMBER DATA_I1,
    UNIFORM(0, 15, RANDOM(22))::NUMBER DATA_I2,
    NULLIF(UNIFORM(0, 15, RANDOM(222)),0)::NUMBER DATA_I3,
    RANDSTR(UNIFORM(1, 10, RANDOM()), RANDOM())::VARCHAR  DATA_A1,
    RANDSTR(ABS(RANDOM()) % 10, RANDOM())::VARCHAR DATA_A2,
    NULLIF(RANDSTR(UNIFORM(0, 10, RANDOM()), RANDOM()),'')::VARCHAR DATA_A3,
    UNIFORM(0, 50, RANDOM(10))::NUMBER VALUE_I1,
    UNIFORM(0, 1500, RANDOM(15))/10::FLOAT VALUE_D1
FROM TABLE(GENERATOR(ROWCOUNT => 50000)) V
ORDER BY 1;
--
UPDATE _TEST_DATA_SOURCE_2
SET DATA_PT = DATA_PATTERN(ARRAY_CONSTRUCT(
    1,
    '_TEST_DATA_SOURCE_2',
    DATE(DATA_TS),
    DATE_PART(HOUR, DATA_TS),
    DATA_TS,
    DATA_I1,
    DATA_I2,
    DATA_I3,
    DATA_A1,
    DATA_A2,
    DATA_A3
));
-------------------------------------------------------
-- Create two dummy aggreagtion data targets
-------------------------------------------------------
--
-- Create dummy aggregation data target 1
--
CREATE OR REPLACE TRANSIENT TABLE _TEST_DATA_TARGET_1 (
	"DATA_PT" 								NUMBER       NOT NULL,
	"DATA_DN" 								VARCHAR,
	"DATA_DT" 								DATE         NOT NULL,
  "DATA_HR" 								NUMBER,
  "DATA_TS" 								TIMESTAMP_NTZ,
	"DATA_I1" 								NUMBER       NOT NULL,
	"DATA_I2" 								NUMBER,
	"DATA_I3" 								NUMBER,
	"DATA_A1" 								VARCHAR,
	"DATA_A2" 								VARCHAR,
	"DATA_A3" 								VARCHAR,
	"VSUM_I1" 								NUMBER,
	"VCNT_I2" 								NUMBER,
	"VSUM_D1" 								FLOAT,
	"VAVG_D2" 								FLOAT
);
--
-- Register the tagegt table 1
--
-- DELETE FROM DATA_AGGREGATION_TARGETS WHERE TARGET_TABLE = '_TEST_DATA_TARGET_1';
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
SELECT 'Test: dummy aggregation target 1'
	,$1
	,$2
	,$3
	,$4
	,DATEADD(HOUR, -3, DATE_TRUNC('HOUR', TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP(0))))
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
	'_TEST_DATA_TARGET_1'
	,'DATA_TS'
	, 5
	,'DATEADD(MINUTE, :2, :1)'
	,'MINUTES'
	-- all group-by columns in source data
	,'["DATA_PATTERN",
		"DATA_NAME",
		"DATA_DATE",
    "DATA_HOUR",
    "DATA_TIME",
		"DATA_I1",
		"DATA_I2",
		"DATA_I3",
		"DATA_A1",
		"DATA_A2",
		"DATA_A3"
      ]'
	-- group-by columns of target data and which source column is the match
	,'["DATA_PT:DATA_PATTERN",
		"DATA_DN:DATA_NAME",
    "DATA_DT:DATA_DATE",
    "DATA_HR:DATA_HOUR",
    "DATA_TS:DATA_TIME",
		"DATA_I1:DATA_I1",
		"DATA_I2:DATA_I2",
		"DATA_I3:DATA_I3",
		"DATA_A1:DATA_A1",
		"DATA_A2:DATA_A2",
		"DATA_A3:DATA_A3"
       ]'
	-- indicators of which group-by column are needed in target table
	,'[1,1,1,1,1,1,1,1,0,0,0]'
	-- aggregate columns of target data and which aggregating column is the match
	,'["VSUM_I1:VALUE_I1","VCNT_I2:VALUE_I2","VSUM_D1:VALUE_D1","VAVG_D2:VALUE_D2"]'
	-- what aggregation function will be used for every aggregation column
	,'["SUM(?)","COUNT(*)","SUM(?)","ROUND(AVG(?),2)"]'
	);
--
-- Register the source data table
--
-- DELETE FROM DATA_AGGREGATION_SOURCES WHERE TARGET_TABLE = '_TEST_DATA_TARGET_1' AND SOURCE_TABLE = '_TEST_DATA_SOURCE_1';
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
SELECT 'Test: dummy aggregation target 1 source 1'
  	,$1
  	,$2
  	,true
  	,0
  	,False
  	,DATEADD(MINUTE, ROUND(DATEDIFF(MINUTE, '2000-01-01',TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()))/5)*5,'2000-01-01')
  	,NULL
  	,$3
FROM VALUES (
		'_TEST_DATA_TARGET_1'
		,'_TEST_DATA_SOURCE_1'
		,'
		SELECT DATA_PT DATA_PATTERN
			,\'_TEST_DATA_SOURCE_1\'::VARCHAR DATA_NAME
			,DATE(DATA_TS) DATA_DATE
      ,DATE_PART(HOUR, DATA_TS) DATA_HOUR
      ,DATEADD(MINUTE, ROUND(DATEDIFF(MINUTE,\'2000-01-01\',DATA_TS)/5)*5,\'2000-01-01\') DATA_TIME
			,DATA_TS
			,DATA_I1
			,DATA_I2
			,DATA_I3
			,DATA_A1
			,DATA_A2
			,DATA_A3
			,VALUE_I1
			,VALUE_I1 VALUE_I2
			,VALUE_D1
			,VALUE_D1 VALUE_D2
		FROM _TEST_DATA_SOURCE_1
		'
		);
--
-- Register the source data table
--
-- DELETE FROM DATA_AGGREGATION_SOURCES WHERE TARGET_TABLE = '_TEST_DATA_TARGET_1' AND SOURCE_TABLE = '_TEST_DATA_SOURCE_2';
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
SELECT 'Test: dummy aggregation target 1 source 2'
  	,$1
  	,$2
  	,true
  	,0
  	,False
    ,DATEADD(MINUTE, ROUND(DATEDIFF(MINUTE, '2000-01-01',TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()))/5)*5,'2000-01-01')
  	,NULL
  	,$3
FROM VALUES (
		'_TEST_DATA_TARGET_1'
		,'_TEST_DATA_SOURCE_2'
		,'
		SELECT DATA_PT DATA_PATTERN
			,\'_TEST_DATA_SOURCE_2\'::VARCHAR DATA_NAME
			,DATE(DATA_TS) DATA_DATE
      ,DATE_PART(HOUR, DATA_TS) DATA_HOUR
      ,DATEADD(MINUTE, ROUND(DATEDIFF(MINUTE,\'2000-01-01\',DATA_TS)/5)*5,\'2000-01-01\') DATA_TIME
			,DATA_TS
			,DATA_I1
			,DATA_I2
			,DATA_I3
			,DATA_A1
			,DATA_A2
			,DATA_A3
			,VALUE_I1
			,VALUE_I1 VALUE_I2
			,VALUE_D1
			,VALUE_D1 VALUE_D2
		FROM _TEST_DATA_SOURCE_2
		'
		);
--
-- Populate summary data of one day
--
CALL DATA_AGGREGATOR('_TEST_DATA_TARGET_1', '2020-01-07', 1);
--
-- Populate summary data of all available dayes
--
CALL DATA_AGGREGATOR('_TEST_DATA_TARGET_1', 0);
--
-- Check result of aggrgation 1
--
select data_ts, count(*) cnt
from _TEST_DATA_TARGET_1
group by 1
order by 1 desc
;
--
-- Create dummy aggregation data target 2
--
CREATE OR REPLACE TRANSIENT TABLE _TEST_DATA_TARGET_2 (
  "DATA_PT" 								NUMBER,
	"DATA_DN" 								VARCHAR,
  "DATA_DT" 								DATE         NOT NULL,
  "DATA_HR" 								NUMBER,
  "DATA_TS" 								TIMESTAMP_NTZ,
	"DATA_I1" 								NUMBER       NOT NULL,
	"DATA_I2" 								NUMBER,
	"DATA_I3" 								NUMBER,
	"DATA_A1" 								VARCHAR,
	"DATA_A2" 								VARCHAR,
	"DATA_A3" 								VARCHAR,
	"VSUM_I1" 								NUMBER,
	"VCNT_I2" 								NUMBER,
	"VSUM_D1" 								FLOAT,
	"VAVG_D2" 								FLOAT
);
--
-- Register the tagegt table 1
--
-- DELETE FROM DATA_AGGREGATION_TARGETS WHERE TARGET_TABLE = '_TEST_DATA_TARGET_1';
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
SELECT 'Test: dummy aggregation target 2'
	,$1
	,$2
	,$3
	,$4
	,DATE_TRUNC('DAY', CURRENT_DATE ()-7)
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
	'_TEST_DATA_TARGET_2'
	,'DATA_TS'
	, 1440
	,'DATEADD(MINUTE, :2, :1)'
	,'DAILY'
	-- all group-by columns in source data
	,'["DATA_PATTERN",
		"DATA_NAME",
    "DATA_DATE",
    "DATA_HOUR",
    "DATA_TIME",
		"DATA_I1",
		"DATA_I2",
		"DATA_I3",
		"DATA_A1",
		"DATA_A2",
		"DATA_A3"
      ]'
	-- group-by columns of target data and which source column is the match
	,'["DATA_DT:DATA_DATE",
		"DATA_I1:DATA_I1",
		"DATA_I2:DATA_I2",
		"DATA_I3:DATA_I3",
		"DATA_A1:DATA_A1",
		"DATA_A2:DATA_A2",
		"DATA_A3:DATA_A3"
       ]'
	-- indicators of which group-by column are needed in target table
	,'[0,0,1,0,0,1,1,1,1,1,1]'
	-- aggregate columns of target data and which aggregating column is the match
	,'["VSUM_I1:VALUE_I1","VCNT_I2:VALUE_I2","VSUM_D1:VALUE_D1","VAVG_D2:VALUE_D2"]'
	-- what aggregation function will be used for every aggregation column
	,'["SUM(?)","COUNT(DISTINCT ?)","SUM(?)","ROUND(AVG(?),2)"]'
	);
--
-- Register the source data table
--
-- DELETE FROM DATA_AGGREGATION_SOURCES WHERE TARGET_TABLE = '_TEST_DATA_TARGET_1' AND SOURCE_TABLE = '_TEST_DATA_SOURCE_1';
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
SELECT 'Test: dummy aggregation target 2 source 1'
    ,$1
    ,$2
    ,true
    ,0
    ,False
    ,DATE_TRUNC('DAY', CURRENT_DATE()) -1
    ,NULL
    ,$3
FROM
VALUES (
		'_TEST_DATA_TARGET_2'
		,'_TEST_DATA_SOURCE_1'
		,'
		SELECT DATA_PT DATA_PATTERN
			,\'_TEST_DATA_SOURCE_1\'::VARCHAR DATA_NAME
			,DATE(DATA_TS) DATA_DATE
      ,DATE_PART(HOUR, DATA_TS) DATA_HOUR
      ,DATEADD(MINUTE, ROUND(DATEDIFF(MINUTE,\'2000-01-01\',DATA_TS)/5)*5,\'2000-01-01\') DATA_TIME
			,DATA_TS
			,DATA_I1
			,DATA_I2
			,DATA_I3
			,DATA_A1
			,DATA_A2
			,DATA_A3
			,VALUE_I1
			,VALUE_I1 VALUE_I2
			,VALUE_D1
			,VALUE_D1 VALUE_D2
		FROM _TEST_DATA_SOURCE_1
		'
		);
--
-- Register the source data table
--
-- DELETE FROM DATA_AGGREGATION_SOURCES WHERE TARGET_TABLE = '_TEST_DATA_TARGET_1' AND SOURCE_TABLE = '_TEST_DATA_SOURCE_2';
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
SELECT 'Test: dummy aggregation target 2 source 2'
    ,$1
    ,$2
    ,true
    ,0
    ,False
    ,DATE_TRUNC('DAY', CURRENT_DATE()) -1
    ,NULL
    ,$3
FROM
VALUES (
		'_TEST_DATA_TARGET_2'
		,'_TEST_DATA_SOURCE_2'
		,'
		SELECT DATA_PT DATA_PATTERN
			,\'_TEST_DATA_SOURCE_2\'::VARCHAR DATA_NAME
			,DATE(DATA_TS) DATA_DATE
      ,DATE_PART(HOUR, DATA_TS) DATA_HOUR
      ,DATEADD(MINUTE, ROUND(DATEDIFF(MINUTE,\'2000-01-01\',DATA_TS)/5)*5,\'2000-01-01\') DATA_TIME
			,DATA_TS
			,DATA_I1
			,DATA_I2
			,DATA_I3
			,DATA_A1
			,DATA_A2
			,DATA_A3
			,VALUE_I1
			,VALUE_I1 VALUE_I2
			,VALUE_D1
			,VALUE_D1 VALUE_D2
		FROM _TEST_DATA_SOURCE_2
		'
		);
--
-- Populate summary data of one day
--
CALL DATA_AGGREGATOR('_TEST_DATA_TARGET_2', '2020-01-07', 1);
--
-- Populate summary data of all available dayes
--
CALL DATA_AGGREGATOR('_TEST_DATA_TARGET_2', 0);
--
-- Check result of aggrgation 2
--
select data_ts, count(*) cnt
from _TEST_DATA_TARGET_2
group by 1
order by 1 desc
;
