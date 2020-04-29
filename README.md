# Automated Data Aggregation (1): Data Aggregator 

Data aggregation is a common data processing method, which is widely used in data analysis, business intelligence and machine learning projects. It is quite often that we need to aggregate the data from many data sources in a very different format, and the data format may change from time to time. It could be a challenge to program a data processing application to handle the varieties of this case. Here we introduce an automation solution that solves the case where all the source data are continuously being loaded into a series of database tables with the different table schemas. 

## Concept Introduction

Let’s compare our data processing case with an automated color paint product line to extract the key elements and control factors of the system, then use these elements and factors  to model our data aggregation.

![An automated product line](images/figure1-an-automated-product-line.jpg?raw=true "An automated product line")
Figure-1 An automated product line

In the above illustrated product line, we have the raw materials continuously coming from three conveyors S1, S2 and S3. The material from S1 is packed in small bags, it takes four to feed in a mixer and produce one bucket of product; the material from S2 is packed in middle size bags, it takes two to feed a mixer and produce one bucket of product; and the material from S3 is packed in large bags, it just needs one to feed a mixer and produce one bucket of product. Every three different buckets of product from three raw sources will be packed together to produce a final product.

In contrast to our data processing case, we can think the conveyors are raw data tables, each of them holds the different format of raw data in different granularities. The processing point is the timestamp of the current raw data ingesting. The available point is the timestamp of the committed raw data. The data processing app will chunk the raw data into batches and join the metadata as the transformation to get the unified data in the same format and same granularity, then aggregate the unified data into the needed granularity in the same format and load the result into a warehouse table. We can summarize two entities and their attributes of our case as Table-1 and Table-2.


Table-1 Target data entity and attributes
Attritue | Type | Description
------------------------|----------------|-------------------------------------------------
Target_Table | Property | The summary date table name
Batch_Control_Column | Property | The column is used to chunk the data
Batch_Control_Size | Property | The miro chunk size in a batch processing
Batch_Control_Next | Method | A function to determine the chunk border
Batch_Processed | Property | A timestamp of the completed processing
Batch_Processing | Property | A timestamp of the stop point of current batch
Batch_Microchunk_Current | Property | A timestamp of the chunk in current batch
Batch_Schedule_Type | Property | The minimum schedule frequency
Batch_Schedule_Last | Property | The timestamp of the last schedule
Pattern_Columns | Property | The unified data formatting columns
Groupby_Columns | Property | The aggregation granularity columns
Groupby_Pattern | Property | A bitwised indicators to groupby columns
Groupby_Flexible | Property | Allow the different granularity in result
Aggregate_Columns | Property | The column list which will aggregate values
Aggregate_Functions | Property | The function list applied to aggregate columns



Table-2 Source data entity and attributes
Attritue | Type | Description
------------------------|----------------|-------------------------------------------------
Target_Table | Property | Which summary data needs this source
Source_Table | Property | The source data table name
Pattern_Default | Property | Default granularity of the data in source table
Parttern_Flexible | Property | Allow multi-granularities exist in one table 
Data_Available_Time | Property | The timestamp of the committed source data
Data_Check_Schedule | Property | Timestamp of the last check of availability
Transformation | Method | A query or view to refactor the data format


## Data Modeling

Based on the previous description, we can easily figure out a very simple data model just having two entities in our case. The entity relationship diagram is illustrated in  Figure-2.


![Entity relationship diagram](images/figure2-entity-relationship-diagram.jpg?raw=true "Entity relationship diagram")
Figure-2 Entity Relationship Diagram


### Target Data Definition

1.TARGET_LABEL (TEXT): 
2.TARGET_TABLE (TEXT): 
3.BATCH_CONTROL_COLUMN (TEXT): 
4.BATCH_CONTROL_SIZE (NUMBER): 
5.BATCH_CONTROL_NEXT (TEXT): 
6.BATCH_PROCESSED (TIMESTAMP_NTZ): 
7.BATCH_PROCESSING (TIMESTAMP_NTZ): 
8.BATCH_MICROCHUNK_CURRENT (TEXT): 
9.BATCH_SCHEDULE _TYPE (TEXT): 
10.BATCH_SCHEDULE_LAST (TIMESTAMP_NTZ): 
11.PATTERN_COLUMNS (ARRAY): 
12.GROUPBY_COLUMNS (ARRAY): 
13.GROUPBY_PATTERN (NUMBER): 
14.GROUPBY_FLEXIBLE (BOOLEAN): 
15.AGGREGATE_COLUMNS (ARRAY): 
16.AGGREGATE_FUNCTIONS (ARRAY): 
17.DEFAULT_PROCEDURE (TEXT): 


### Source Data Definition

1.SOURCE_LABEL (TEXT): 
2.TARGET_TABLE (TEXT): 
3.SOURCE_TABLE (TEXT): 
4.SOURCE_ENABLED (BOOLEAN): 
5.PATTERN_DEFAULT (NUMBER): 
6.PATTERN_FLEXIBLE (BOOLEAN): 
7.DATA_AVAILABLETIME (TIMESTAMP_NTZ): 
8.DATA_CHECKSCHEDULE (TIMESTAMP_NTZ): 
9.TRANSFORMATION (TEXT): 

## Code Implementation

As all raw data have been ingested into a snowflake warehouse already, we will implement the processing with snowflake stored procedures and functions. Snowflake supports a simplified javascript API, the functions and procedures can be programmed in javascript, so that most javascript programmers involve the work quickly with a very short time learning.

### Single Chunk Process 

![Single chunk processing flowchart](images/figure3-single-chunk-processing.jpg?raw=true "Single chunk processing flowchart")
Figure-3 Flowchart for single chunk processing

### Loop The Chunks Process

![Loop multi-chunks processing](images/figure4-loop-multi-chuks-processing.jpgraw=true "Loop multi-chunks processing flowchart")
Figure-4 Flowchart for loop multi-chunks processing

## Aggregation Setup

Here we are going to set up a revenue data aggregation/allocation for Exec Reports. The target table namly “BI.ACCOUNT_DATA.SELLSIDE_ACCOUNT_DATA_DAILY” is created by running the following SQL script, then we go through the listed steps in following sections to complete the full setup.

```
CREATE OR REPLACE TABLE BI.ACCOUNT_DATA.SELLSIDE_ACCOUNT_DATA_DAILY (
	"DATA_TS" 						DATE
	,"BUSINESS_UNIT_DETAIL_ID" 		NUMBER
	,"PRODUCT_LINE_ID" 	        	NUMBER
	,"NETWORK_NAME_ID" 				NUMBER
	,"DEVICE_TYPE_ID" 				NUMBER
	,"CONTRACT_ID" 					NUMBER
	,"CURRENCY_CODE" 				VARCHAR(10)
	,"CONVERSION_RATE" 				FLOAT
	,"REVSHARE" 					FLOAT
	,"BIDDED_SEARCHES" 				NUMBER
	,"CLICKS" 						NUMBER
	,"GROSS_REVENUE" 				FLOAT
	);
```

### Aggregation Target Setup

--
DELETE FROM BI._CONTROL_LOGIC.DATA_AGGREGATION_TARGETS
WHERE TARGET_TABLE = 'BI.ACCOUNT_DATA.SELLSIDE_ACCOUNT_DATA_DAILY';
--
SELECT *
FROM BI._CONTROL_LOGIC.DATA_AGGREGATION_TARGETS
WHERE TARGET_TABLE = 'BI.ACCOUNT_DATA.SELLSIDE_ACCOUNT_DATA_DAILY';
--

INSERT INTO BI._CONTROL_LOGIC.DATA_AGGREGATION_TARGETS (
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
SELECT 'For_Exec_Report'
	,$1
	,$2
	,$3
	,$4
	,date_trunc('MONTH', CURRENT_DATE ()) - 1
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
	'BI.ACCOUNT_DATA.SELLSIDE_ACCOUNT_DATA_DAILY'
	,'DATA_TS'
	,1440
	,'DATEADD(MINUTE, :2, :1)'
	,'DAILY'
	-- all group-by columns in source data
	,'["DATA_PATTERN",
       "DATA_TS",
       "BUSINESS_UNIT_DETAIL_ID",
       "PRODUCT_LINE_ID",
       "NETWORK_NAME_ID",
       "DEVICE_TYPE_ID",
       "CONTRACT_ID",
       "CURRENCY_CODE",
       "CONVERSION_RATE"
       ]'
	-- group-by columns of target data and which source column is the match
	,'["DATA_TS:DATA_TS",
       "BUSINESS_UNIT_DETAIL_ID:BUSINESS_UNIT_DETAIL_ID",
       "PRODUCT_LINE_ID:PRODUCT_LINE_ID",
       "NETWORK_NAME_ID:NETWORK_NAME_ID",
       "DEVICE_TYPE_ID:DEVICE_TYPE_ID",
       "CONTRACT_ID:CONTRACT_ID",
       "CURRENCY_CODE:CURRENCY_CODE",
       "CONVERSION_RATE:CONVERSION_RATE"
       ]'
	-- indicators of which group-by column are needed in target table
	,'[0,1,1,1,1,1,1,1,1]'
	-- aggregate columns of target data and which aggregating column is the match
	,'["REVSHARE:REVSHARE","BIDDED_SEARCHES:BIDDED_SEARCHES","CLICKS:CLICKS","GROSS_REVENUE:GROSS_REVENUE"]'
	-- what aggregation function will be used for every aggregation column
	,'["AVG(?)","SUM(?)","SUM(?)","SUM(?)"]'
);


### Aggregation Source Setup

-- visual check
SELECT *
FROM BI._CONTROL_LOGIC.DATA_AGGREGATION_SOURCES
WHERE TARGET_TABLE = 'BI.ACCOUNT_DATA.SELLSIDE_ACCOUNT_DATA_DAILY';

-- delete a un-needed source
DELETE FROM BI._CONTROL_LOGIC.DATA_AGGREGATION_SOURCES 
WHERE TARGET_TABLE = 'BI.ACCOUNT_DATA.SELLSIDE_ACCOUNT_DATA_DAILY'
AND SOURCE_TABLE = 'DATAMART.SELLSIDE_NETWORK.GOOGLE_ADSENSE_DRID_PLATFORM_DAILY'
;

-- Re Add the new source in
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
-- cascade views is qool revenue, going through the OM google account, the rest is all syndication
SELECT 'Syndication and some Qool data - using drid data'
 	,$1
	,$2
	,False
	,0
	,False
	,CURRENT_DATE() - 7
	,NULL
	,$3
FROM
VALUES (
	'BI.ACCOUNT_DATA.SELLSIDE_ACCOUNT_DATA_DAILY'
	,'DATAMART.SELLSIDE_NETWORK.GOOGLE_ADSENSE_DRID_PLATFORM_DAILY'
	,'
      --Syndication and some Qool data - using drid data
      SELECT DATA_PATTERN(PARSE_JSON(\'[0,1,1,1,1,1,1,1,1]\')) DATA_PATTERN
          ,data_ts::DATE data_ts
          ,ssc.contract_id
          ,dtm.device_type_id
          ,CASE WHEN lower(p.parent_partner_name) = \'cascade views\' THEN 1 ELSE 6 END business_unit_detail_id
          ,CASE WHEN lower(p.parent_partner_name) = \'cascade views\' THEN 1 ELSE 6 END PRODUCT_LINE_ID
          ,\'USD\' currency_code
          ,1 conversion_rate
          ,ssc.network_id network_name_id
          ,0.595 revshare
          ,sum(ad_requests) bidded_searches
          ,sum(clicks) clicks
          ,sum(earnings_usd) gross_revenue
      FROM DATAMART.SELLSIDE_NETWORK.GOOGLE_ADSENSE_DRID_PLATFORM_DAILY g
      JOIN bi.common.device_type_mappings dtm
          ON lower(g.platform_type_code) = dtm.device_type_string
      JOIN bi.common.sellside_contracts ssc
          ON g.account_id = ssc.account_id_lookup_value
      JOIN datamart.common.partner p
          ON g.domain_registrant = p.drid
      WHERE data_ts >= :1 AND data_ts < dateadd(minute, :2, :1)
      GROUP BY 1,2,3,4,5,6,7,8,9,10
      '
);


## Schedule The Process


### Kick a Manual Run

#### Run a Single chunk process

-- Manually test one day's data population to test settings
CALL DATA_AGGREGATOR('BI.ACCOUNT_DATA.SELLSIDE_ACCOUNT_DATA_DAILY', '2019-12-31', 0);


#### Run a batch period process

-- Setup an initial starting date for next auto-run
UPDATE BI._CONTROL_LOGIC.DATA_AGGREGATION_TARGETS
SET BATCH_PROCESSED = '2019-12-31'
	,BATCH_PROCESSING = NULL
WHERE TARGET_TABLE = 'BI.ACCOUNT_DATA.SELLSIDE_ACCOUNT_DATA_DAILY';

-- Automate the Aggregate data population
CALL DATA_AGGREGATOR('BI.ACCOUNT_DATA.SELLSIDE_ACCOUNT_DATA_DAILY', 0);

### Schedule by Tasks

#### Sliding Period Task

-- Create root task with a schedule
CREATE OR REPLACE TASK HOURLY_POPULATE_SELLSIDE_DATA_AVAILABILITY
    WAREHOUSE = S1_BI
    SCHEDULE = 'USING CRON 0 9-18 * * * America/Los_Angeles'
AS
UPDATE DATA_AGGREGATION_SOURCES 
SET DATA_AVAILABLETIME = DATEADD(DAY, -60, CURRENT_DATE())
WHERE TARGET_TABLE = 'BI.ACCOUNT_DATA.SELLSIDE_ACCOUNT_DATA_DAILY';

-- Create follower task with "after" cause
CREATE  OR REPLACE TASK HOURLY_POPULATE_SELLSIDE_ACCOUNT_DATA
  WAREHOUSE = S1_BI
  AFTER HOURLY_POPULATE_SELLSIDE_DATA_AVAILABILITY
AS
CALL DATA_AGGREGATOR ('BI.ACCOUNT_DATA.SELLSIDE_ACCOUNT_DATA_DAILY', 0);

-- Enable the root taks for hourly scheduled
SELECT SYSTEM$TASK_DEPENDENTS_ENABLE('HOURLY_POPULATE_SELLSIDE_DATA_AVAILABILITY');

#### Full Automated Task

-- Create root task with a schedule
CREATE OR REPLACE TASK HOURLY_POPULATE_SELLSIDE_DATA_AVAILABILITY
    WAREHOUSE = S1_BI
    SCHEDULE = 'USING CRON 0 9-18 * * * America/Los_Angeles'
AS
( Call a procedure to refresh the available time of all sources here. This SP does not exist yet.);

-- Create follower task with "after" cause
CREATE  OR REPLACE TASK HOURLY_POPULATE_SELLSIDE_ACCOUNT_DATA
  WAREHOUSE = S1_BI
  AFTER HOURLY_POPULATE_SELLSIDE_DATA_AVAILABILITY
AS
CALL DATA_AGGREGATOR ('BI.ACCOUNT_DATA.SELLSIDE_ACCOUNT_DATA_DAILY', 0);

-- Enable the root taks for hourly scheduled
SELECT SYSTEM$TASK_DEPENDENTS_ENABLE('HOURLY_POPULATE_SELLSIDE_DATA_AVAILABILITY');

## Author



## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details

## Acknowledgments

* Hat tip to anyone whose code was used
* Inspiration
* etc

