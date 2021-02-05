!set variable_substitution=true;
!define ver=V20;
--
use database &{db_name};
create schema if not exists &{sc_name};
--create schema &{sc_name};
use schema &{sc_name};
--
--=====================================================
-- Create task management tables
--=====================================================
--
-- DROP SEQUENCE DATA_AGGREGATION_TARGETS_SEQ;
--
CREATE SEQUENCE DATA_AGGREGATION_TARGETS_SEQ START = 1 INCREMENT = 1;
--
-- DROP TABLE DATA_AGGREGATION_TARGETS;
--
CREATE TABLE DATA_AGGREGATION_TARGETS
(
	TARGET_ID 					NUMBER NOT NULL DEFAULT DATA_AGGREGATION_TARGETS_SEQ.NEXTVAL,
	TARGET_LABEL				TEXT,
	TARGET_DATA					TEXT NOT NULL,
	BATCH_CONTROL_COLUMN		TEXT,
	BATCH_CONTROL_SIZE			NUMBER,
	--BATCH_CONTROL_NEXT			TEXT,
	BATCH_PROCESSED		    	TIMESTAMP_NTZ,
	BATCH_PROCESSING			TIMESTAMP_NTZ,
	BATCH_MICROCHUNK_CURRENT 	TIMESTAMP_NTZ,
	BATCH_SCHEDULE_TYPE			TEXT,
	BATCH_SCHEDULE_LAST			TIMESTAMP_NTZ,
	PATTERN_COLUMNS		    	ARRAY,
	GROUPBY_COLUMNS		    	ARRAY,
	GROUPBY_PATTERN		    	NUMBER,
	GROUPBY_FLEXIBLE			BOOLEAN,
	AGGREGATE_COLUMNS			ARRAY,
	AGGREGATE_FUNCTIONS			ARRAY,
	SUPPORT_SP_VERSIONS			ARRAY,
	CONSTRAINT PK_DATA_AGGREGATION_TARGETS PRIMARY KEY (TARGET_ID)
)
CLUSTER BY (TARGET_DATA)
COMMENT = 'This table is used to register the aggregation targets'
;
--
-- DROP SEQUENCE DATA_AGGREGATION_SOURCES_SEQ;
--
CREATE SEQUENCE DATA_AGGREGATION_SOURCES_SEQ START = 1 INCREMENT = 1;
--
-- DROP TABLE DATA_AGGREGATION_SOURCES;
--
CREATE TABLE DATA_AGGREGATION_SOURCES
(
	SOURCE_ID 					NUMBER NOT NULL DEFAULT DATA_AGGREGATION_SOURCES_SEQ.NEXTVAL,
	TARGET_ID	        		NUMBER NOT NULL,
	SOURCE_LABEL				TEXT,
	SOURCE_DATA	        		TEXT NOT NULL,
	SOURCE_ENABLED	        	BOOLEAN,
	SOURCE_READY_TIME	    	TIMESTAMP_NTZ,
	SOURCE_CHECK_TIME	    	TIMESTAMP_NTZ,
	SOURCE_CHECK_QUERY	        TEXT,
	PATTERN_DEFAULT	        	NUMBER,
	PATTERN_FLEXIBLE	    	BOOLEAN,
	TRANSFORMATION	        	TEXT,
	CONSTRAINT PK_DATA_AGGREGATION_SOURCES PRIMARY KEY (SOURCE_ID),
	CONSTRAINT FK_DATA_AGGREGATION_SOURCES_TARGET_DATA FOREIGN KEY (TARGET_ID)
		REFERENCES DATA_AGGREGATION_TARGETS(TARGET_ID)
)
CLUSTER BY (TARGET_ID, SOURCE_DATA)
COMMENT = 'This table is used to register the aggregation sources'
;
--
-- DROP SEQUENCE DATA_AGGREGATION_LOGGING_SEQ;
--
CREATE SEQUENCE DATA_AGGREGATION_LOGGING_SEQ START = 1 INCREMENT = 1;
--
-- DROP TABLE DATA_AGGREGATION_LOGGING;
--
CREATE TABLE DATA_AGGREGATION_LOGGING
(
	EVENT_ID 					NUMBER NOT NULL DEFAULT DATA_AGGREGATION_LOGGING_SEQ.NEXTVAL,
	EVENT_TIME	    	        TIMESTAMP_NTZ DEFAULT TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP),
	EVENT_TARGET	        	TEXT,
	EVENT_SOURCE	        	TEXT,
	EVENT_STATUS				TEXT,
	EVENT_STATE					TEXT,
	EVENT_QUERY					TEXT
)
COMMENT = 'This table is used to log the error of running the processing'
;
!set variable_substitution=false;
--=====================================================
-- Create assisstant functions
--=====================================================
--
-- DROP FUNCTION DATA_PATTERN(ARRAY);
--
CREATE FUNCTION DATA_PATTERN(
	P ARRAY
	)
RETURNS DOUBLE
LANGUAGE JAVASCRIPT
AS
$$
if (typeof P !== "undefined" || P !== null) {
	datPat = 0, misBit = 0;
	if (typeof P[0] === "object") {
		Q = P[0]["pattern_columns"];
		R = P[0]["groupby_columns"];
		if (typeof Q !== "undefined" || Q !== null || typeof R !== "undefined" || R !== null) {
			patLen = Q.length;
			for (i = 0; i < patLen; i++) {
				if (R.indexOf(Q[i]) !== -1) {
					misBit = 0;
				} else {
					misBit = 1;
				}
				datPat = 2 * datPat + misBit;
			}
		}
	} else {
		patLen = P.length;
		for (i = 0; i < patLen; i++) {
			if (P[i]) {
				misBit = 0;
			} else {
				misBit = 1;
			}
			datPat = 2 * datPat + misBit;
		}
	}
}
return datPat;
$$;
--
--
-- DROP FUNCTION COLUMN_MAP(ARRAY);
--
CREATE FUNCTION COLUMN_MAP(
	P ARRAY
	)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
AS
$$
mapping = {};
if (P !== "undefined" || P !== null) {
	Q = P[0];
	if (Q["target_column_list"] !== "undefined" && typeof Q["target_column_list"] === "object"
		&& Q["source_column_list"] !== "undefined" && typeof Q["source_column_list"] === "object"
		&& Q["target_column_list"].length === Q["source_column_list"].length
	) {
		patLen = Q["target_column_list"].length;
		for (i = 0; i < patLen; i++) {
			mapping[Q["target_column_list"][i]] = Q["source_column_list"][i];
		}
	}
}
return mapping;
$$;
--
--
-- DROP FUNCTION REVENUE_SHARE(VARIANT, VARCHAR, FLOAT);
--
CREATE FUNCTION REVENUE_SHARE(
	P VARIANT,
	D VARCHAR,
	V FLOAT
	)
RETURNS FLOAT
LANGUAGE JAVASCRIPT
AS
$$
var rev_share = null, Q = D;
if (D) { Q = D.toUpperCase(); }
if (typeof P === "undefined") {
	rev_share = null;
}
else if (typeof P === "number") {
	rev_share = P;
}
else if (typeof P[0] === "object" && typeof V !== "undefined") {
	rev_share = P.filter(x => (x["RANGE_LOWER"] <= V && (!x["RANGE_UPPER"] || x["RANGE_UPPER"] > V)))[0]["REVENUE_SHARE"];
}
else if (typeof P === "object") {
	if (typeof P[Q] === "number") {
		rev_share = P[Q];
	}
	else if (typeof P[Q] === "undefined" && typeof P["(Others)"] === "undefined") {
		rev_share = P["(OTHERS)"];
	}
}
return rev_share;
$$;
--
--
-- DROP FUNCTION REVENUE_SHARE(VARIANT, VARCHAR);
--
CREATE FUNCTION REVENUE_SHARE(
	P VARIANT,
	D VARCHAR
	)
RETURNS FLOAT
LANGUAGE JAVASCRIPT
AS
$$
var rev_share = null, Q = D;
if (D) { Q = D.toLowerCase(); if (["phone", "smart phones"].includes(Q)) { Q = 'mobile' } }
if (typeof P === "undefined") {
	rev_share = null;
}
else if (typeof P === "number") {
	rev_share = P;
}
else if (typeof P === "object") {
	for (item in P) {
		if (P[item][Q] && typeof P[item][Q][0] === "number") {
			rev_share = P[item][Q][0];
			break;
		}
	}
	if (!rev_share) {
		for (item in P) {
			if (P[item]["other"] && typeof P[item]["other"][0] === "number") {
				rev_share = P[item]["other"][0];
				break;
			}
		}
	}
}
return rev_share;
$$;
--
--=====================================================
-- Create aggregator stored procedures
--=====================================================
--
-------------------------------------------------------
-- Chunk processor procedue to handle a chunk of data over all sources
-------------------------------------------------------
-- DROP PROCEDURE DATA_AGGREGATOR(VARCHAR, BOOLEAN, BOOLEAN, BOOLEAN, VARCHAR);
CREATE OR REPLACE PROCEDURE  DATA_AGGREGATOR (
	TARGET_DATA VARCHAR,
	SCRIPT_ONLY BOOLEAN,
	LOG_DETAILS BOOLEAN,
	NON_ENABLED BOOLEAN,
	CHUNK_TAG VARCHAR
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT STRICT
AS
$$
/**
 * Generate a simplified non-standard AST from a SQL syntax
 * @param {string} sqlSyntax, {object} astParser
 * @return {object>}
 */
function sqlToAst(sqlSyntax, astParser) {
    var sqlAst = {};
	var sqlRegex = /(?<=\<\b(?![\s|=|>|0-9]))(.*?)(?=\>)/g;
	var items = [...new Set(sqlSyntax.match(sqlRegex))].sort();
	for (var i = 0; i < items.length; i++) {
		sqlAst[items[i]] = astParser[items[i]] === undefined ? '/*' + items[i] + '*/' : astParser[items[i]];
	}
    return sqlAst;
}

/**
 * Generate a SQL statement from a simplified non-standard AST
 * @param {string} sqlSyntax, {object} sqlAst
 * @return {string>}
 */
function astToSql(sqlSyntax, sqlAst) {
    var astKeys = Object.keys(sqlAst);
	var sqlStatement = sqlSyntax;
	for (var i = 0; i < astKeys.length; i++) {
        var key = astKeys[i];
		sqlStatement = sqlStatement.split('<' + key + '>').join(sqlAst[key]);
	}
	return sqlStatement;
}

/**
 * Get a datetime by adding an interval to another (SQL DATEADD)
 * @param {string} datePart, {number} interval, {string} dateTime
 * @return {string>}
 */
function dateAdd(datePart, interval, dateTime) {
    var result = new Date(dateTime);
    switch (datePart.toUpperCase()) {
      case 'YEAR':
        result.setFullYear(result.getFullYear() + interval);
        break;
      case 'MONTH':
        result.setMonth(result.getMonth() + interval);
        break;
      case 'DAY':
        result.setDate(result.getDate() + interval);
        break;
      case 'HOUR':
        result.setHours(result.getHours() + interval);
        break;
      case 'MINUTE':
        result.setMinutes(result.getMinutes() + interval);
        break;
      case 'SECOND':
        result.setSeconds(result.getSeconds() + interval);
        break;
      default:
        result.setDate(result.getDate() + interval);
    }
	return result;
}

/**
 * Define the SQL templates of the dialect of snowflake.
 * @enum {string}
 */
var sqlDialect = {};
sqlDialect.insert = `INSERT INTO <targetData>(<dimensionList>,<measureColumns>) \n`
	+ `    SELECT <selectList>,<aggregateColumns> \n`
	+ `        FROM <transformation>;`;
sqlDialect.delete = `DELETE FROM <targetData> \n`
	+ `    WHERE <batchControlColumn> >= :1 AND <batchControlColumn> < :3;`;
sqlDialect.update = ``;
sqlDialect.merge = `MERGE INTO <targetData> <targetAlias> \n`
	+ `USING ( \n`
	+ `  SELECT <groupByList>,<aggregateList> \n`
	+ `  FROM ( \n`
	+ `    SELECT <selectList>,<aggregateColumns> \n`
	+ `    FROM <transformation> \n`
	+ `    WHERE <batchControlColumn> >= :1 AND <batchControlColumn> < :3 \n`
	+ `    ) \n`
	+ `  GROUP BY <groupByList>\n`
	+ `  ) <sourceAlias> \n`
	+ `ON <universeJoinList> \n`
	+ `WHEN MATCHED THEN UPDATE SET <measureUpdateList> \n`
	+ `WHEN NOT MATCHED THEN INSERT(<dimensionList>,<measureColumns>) \n`
	+ `VALUES (<aliasedGroupByList>,<aliasedMeasureList>);`;

/**
 * Get chunk processing config data from control database
 * @enum {object}
 */
var sqlScript = '', pageBreaker = '', templateName = 'insert';
var sourceQuery = `SELECT
	  d.TARGET_DATA,
	  d.BATCH_CONTROL_COLUMN,
	  d.BATCH_CONTROL_SIZE,
	  d.BATCH_SCHEDULE_TYPE,
	  d.PATTERN_COLUMNS,
	  d.GROUPBY_COLUMNS,
	  CASE WHEN GROUPBY_FLEXIBLE THEN BITOR(d.GROUPBY_PATTERN, s.PATTERN_DEFAULT) ELSE d.GROUPBY_PATTERN END GROUPBY_PATTERN,
	  d.GROUPBY_FLEXIBLE OR (d.GROUPBY_PATTERN = BITOR(d.GROUPBY_PATTERN, s.PATTERN_DEFAULT)) GROUPBY_COMPITABLE,
	  d.GROUPBY_FLEXIBLE AND s.PATTERN_FLEXIBLE PATTERN_FLEXIBLE,
	  d.AGGREGATE_COLUMNS,
	  d.AGGREGATE_FUNCTIONS,
	  d.SUPPORT_SP_VERSIONS,
	  s.SOURCE_LABEL,
	  s.SOURCE_DATA,
	  s.TRANSFORMATION
  FROM DATA_AGGREGATION_TARGETS d
  JOIN DATA_AGGREGATION_SOURCES s
  USING(TARGET_ID)
  WHERE d.TARGET_DATA = :1
	AND s.SOURCE_ENABLED != :2;`;

var sourceStmt = snowflake.createStatement({
	sqlText: sourceQuery,
	binds: [
		TARGET_DATA, 
		NON_ENABLED
	]
});

var sources = sourceStmt.execute();

/**
 * Loop and process all enabled sources
 */
while (sources.next()) {
	var targetData = sources.getColumnValue(1);
	var batchControlColumn = sources.getColumnValue(2);
	var batchControlSize = sources.getColumnValue(3);
	var batchScheduleType = sources.getColumnValue(4);
	var patternColumns = sources.getColumnValue(5);
	var groupByColumns = sources.getColumnValue(6).map(x => x.split(':')[1]);
	var dimensionColumns = sources.getColumnValue(6).map(x => x.split(':')[0]);
	var groupByPattern = sources.getColumnValue(7);
	var groupByCompitable = sources.getColumnValue(8);
	var patternFlexible = sources.getColumnValue(9);
	var aggregateColumns = sources.getColumnValue(10).map(x => x.split(':')[1]);
	var measureColumns = sources.getColumnValue(10).map(x => x.split(':')[0]);
	var aggregateFunctions = sources.getColumnValue(11);
	var supportSpVersions = sources.getColumnValue(12);
	var sourceLabel = sources.getColumnValue(13);
	var sourceData = sources.getColumnValue(14);
	var transformation = sources.getColumnValue(15);
	var sourceTitle = '',
		sqlExecuted = '',
		sqlStatus = '',
		sqlResult = '(SP call parameter script_only is presented true)';

	if (transformation) { transformation = '(' + transformation + ')' } else { transformation = sourceData }
	var loadQueryTemplate = sqlDialect[templateName];
	var lengthOfISO = (new RegExp('HOUR|MINUTE|SECOND')).test(batchScheduleType) ? 19 : 10;
	var dateTimeISO = dateAdd(batchScheduleType, 0, CHUNK_TAG).toISOString().substring(0, lengthOfISO);
	var chunkEndISO = dateAdd(batchScheduleType, batchControlSize, CHUNK_TAG).toISOString().substring(0, lengthOfISO);

	/** fix hard-coded "minute" of chunk_end calculation in transformation query */
	var chunkMinute = 60 * 24 * batchControlSize;

	/**
	 * Process the data from one source
	 */
	if (groupByCompitable) {
		/**
		 * Construct the SQL syntax elements parser object.
		 * @enum {string}
		 */
		var astParser = {
			aggregateColumns: aggregateColumns,
			aggregateList: '',
			aliasedGroupByList: '',
			aliasedMeasureList: '',
			batchControlColumn: batchControlColumn,
			dimensionList: '',
			groupByList: '',
			measureColumns: measureColumns,
			measureUpdateList: '',
			selectList: '',
			sourceAlias: 'S',
			targetAlias: 'T',
			targetData: targetData,
			transformation: transformation,
			universeJoinList: ''
		};
		astParser.selectList = groupByColumns[0] === "DATA_PATTERN" 
			? ( patternFlexible 
				? 'BITOR(' + groupByColumns[0] + ',' + groupByPattern + ')' 
				: groupByPattern
				) + ' ' 
			: '';
		var flagIndexLast = patternColumns.length - 1,
			patternSegment = groupByPattern,
			columnSplitter = '';
		for (var i = 0; i <= flagIndexLast; i++) {
			var flagPower = 2 ** (flagIndexLast - i);
			if (patternSegment / flagPower < 1) {
				astParser.dimensionList = astParser.dimensionList + columnSplitter + dimensionColumns[groupByColumns.indexOf(patternColumns[i])];
				astParser.selectList = astParser.selectList + columnSplitter + patternColumns[i];
				astParser.groupByList = astParser.groupByList + columnSplitter + patternColumns[i];
				columnSplitter = ',';
			}
			patternSegment %= flagPower;
		}
		astParser.aggregateList = aggregateFunctions.map((x, i) => { return x.replace('?', aggregateColumns[i]) + ' ' + measureColumns[i] });
		astParser.measureUpdateList = measureColumns.map((x, i) => { return x + ' = ' + astParser.sourceAlias + '.' + x });
		astParser.aliasedMeasureList = measureColumns.map(x => { return astParser.sourceAlias + '.' + x });
		astParser.aliasedGroupByList = astParser.groupByList.split(',').map(x => { return astParser.sourceAlias + '.' + x });
		astParser.universeJoinList = astParser.dimensionList.split(',').map((x, i) => { 
			return `COALESCE(TO_CHAR(` 
					+ astParser.targetAlias + '.' + x 
				+ `),'') = COALESCE(TO_CHAR(` 
					+ astParser.sourceAlias + '.' + astParser.groupByList.split(',')[i] 
				+ `),'')` 
		}).join('\n AND ');

		/**
		 * Parse the syntax to simplified non-standard AST
		 * then generate the final SQL statement from the it
		 */
		var sqlAST = sqlToAst(loadQueryTemplate, astParser);
		var loadQuery = astToSql(loadQueryTemplate, sqlAST);

		/**
		 * Generate the log information for the executed SQL
		 */
		sqlExecuted = loadQuery
			.replace(/:3/g, "'" + chunkEndISO + "'")
			.replace(/:2/g, chunkMinute)
			.replace(/:1/g, "'" + dateTimeISO + "'");

		/**
		 * Execute the SQL statement against the warehouse database 
		 * then log the execution information accordingly
		 */
		if (!SCRIPT_ONLY) {
			try {
				var loadStmt = snowflake.createStatement({
					sqlText: loadQuery,
					binds: [
						dateTimeISO, 
						chunkMinute, 
						chunkEndISO
					]
				});
				loadStmt.execute();
				sqlStatus = 'SUCCESS';
				sqlResult = '[INFO-1] Successfully loaded data into target table'
			}
			catch (err) {
				sqlStatus = 'FAILURE';
				sqlResult = '[FAIL] Failure to load data into target table => ' + err
			}
			finally {
				if (LOG_DETAILS || sqlStatus.startsWith('FAIL')) {
					var logQuery = 'INSERT INTO DATA_AGGREGATION_LOGGING(EVENT_TARGET, EVENT_SOURCE, EVENT_STATUS, EVENT_STATE, EVENT_QUERY) \n'
							+  'VALUES(:1, :2, :3, :4, :5)';
					var logStmt = snowflake.createStatement({
						sqlText: logQuery,
						binds: [
							targetData, 
							sourceData, 
							sqlStatus, 
							sqlResult, 
							sqlExecuted
						]
					});
					logStmt.execute()
				}
			}
		}
	}
	else {
		sqlExecuted = '-- No data is loaded from this source as the data pattern is incompatible!';
	}

	/**
	 * Generate the output script for manual execution check
	 */
	sourceTitle = pageBreaker + '-'.repeat(65)
		+ `\n-- SOURCE_LABEL: ` + sourceLabel
		+ `\n-- SOURCE_DATA: ` + sourceData.replace('DATAMART.BUYSIDE_NETWORK.', '').replace('DATAMART.SELLSIDE_NETWORK.', '')
		+ `\n-- SOURCE STATE: ` + sqlResult
		+ `\n` + '-'.repeat(65) + `\n`;
	sqlScript = sqlScript + sourceTitle + sqlExecuted;
	pageBreaker = `\n\n`;
}

/**
 * Return the full script
 * @return loadQuery; 
 */
return sqlScript;
$$;
-------------------------------------------------------
-- Batch processor procedue to handle all source data chunking
-------------------------------------------------------
-- DROP PROCEDURE DATA_AGGREGATOR(VARCHAR, BOOLEAN, BOOLEAN);
--
CREATE OR REPLACE PROCEDURE DATA_AGGREGATOR (
	TARGET_DATA VARCHAR,
	SCRIPT_ONLY BOOLEAN,
	LOG_DETAILS BOOLEAN
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT STRICT
AS
$$
var NON_ENABLED = 0;

/**
 * Generate a simplified non-standard AST from a SQL syntax
 * @param {string} sqlSyntax, {object} astParser
 * @return {object>}
 */
function sqlToAst(sqlSyntax, astParser) {
    var sqlAst = {};
	var sqlRegex = /(?<=\<\b(?![\s|=|>|0-9]))(.*?)(?=\>)/g;
	var items = [...new Set(sqlSyntax.match(sqlRegex))].sort();
	for (var i = 0; i < items.length; i++) {
		sqlAst[items[i]] = astParser[items[i]] === undefined ? '/*' + items[i] + '*/' : astParser[items[i]];
	}
    return sqlAst;
}

/**
 * Generate a SQL statement from a simplified non-standard AST
 * @param {string} sqlSyntax, {object} sqlAst
 * @return {string>}
 */
function astToSql(sqlSyntax, sqlAst) {
    var astKeys = Object.keys(sqlAst);
	var sqlStatement = sqlSyntax;
	for (var i = 0; i < astKeys.length; i++) {
        var key = astKeys[i];
		sqlStatement = sqlStatement.split('<' + key + '>').join(sqlAst[key]);
	}
	return sqlStatement;
}

/**
 * Get a datetime by adding an interval to another (SQL DATEADD)
 * @param {string} datePart, {number} interval, {string} dateTime
 * @return {string>}
 */
function dateAdd(datePart, interval, dateTime) {
    var result = new Date(dateTime);
    switch (datePart.toUpperCase()) {
      case 'YEAR':
        result.setFullYear(result.getFullYear() + interval);
        break;
      case 'MONTH':
        result.setMonth(result.getMonth() + interval);
        break;
      case 'DAY':
        result.setDate(result.getDate() + interval);
        break;
      case 'HOUR':
        result.setHours(result.getHours() + interval);
        break;
      case 'MINUTE':
        result.setMinutes(result.getMinutes() + interval);
        break;
      case 'SECOND':
        result.setSeconds(result.getSeconds() + interval);
        break;
      default:
        result.setDate(result.getDate() + interval);
    }
	return result;
}

/**
 * Define the SQL templates of the dialect of snowflake.
 * @enum {string}
 */
var sqlDialect = {};
sqlDialect.delete = `DELETE FROM <targetData> WHERE <batchControlColumn> >= :1 AND <batchControlColumn> < :2;\n`;

/**
 * Initialize the execution context.
 */
var batchControlColumn = '',
	batchControlSize = 0,
	batchScheduleType = '',
	chunkLoopTag = '',
	chunkLoopEnd = '',
	batchScheduleCurrent;
var loopScript = '',
	pageBreaker = '',
	loopSegmenter = '',
	callStatus = '',
	callResult = '(SP call parameter script_only is presented true)';
var templateName = 'delete';

/**
 * Detect runable or not
 */
var targetQuery = `SELECT BATCH_CONTROL_COLUMN,
  BATCH_CONTROL_SIZE,
  BATCH_SCHEDULE_TYPE,
  DATEADD(MINUTE, CASE 
	  WHEN BATCH_SCHEDULE_TYPE LIKE 'MINUTE%' THEN BATCH_CONTROL_SIZE
	  WHEN BATCH_SCHEDULE_TYPE LIKE 'HOUR%' THEN 60
	  ELSE 1440
	END, BATCH_PROCESSED) CHUNK_LOOP_BEGIN,
  BATCH_POSSIBLE CHUNK_LOOP_END,
  BATCH_SCHEDULE_CURRENT
FROM (
  SELECT BATCH_CONTROL_COLUMN,
    BATCH_CONTROL_SIZE,
    --BATCH_SCHEDULE_TYPE,
    BATCH_PROCESSED,
    BATCH_PROCESSING,
    BATCH_SCHEDULE_TYPE,
    BATCH_SCHEDULE_LAST,
    CURRENT_TIMESTAMP() BATCH_SCHEDULE_CURRENT,
    CASE BATCH_SCHEDULE_TYPE
      WHEN 'HOURLY' THEN DATE_TRUNC(HOUR, BATCH_SCHEDULE_NOW)
      WHEN 'DAILY' THEN DATE_TRUNC(DAY, BATCH_SCHEDULE_NOW)
      ELSE DATEADD(MINUTE,FLOOR(DATEDIFF(MINUTE,'1970-01-01',BATCH_SCHEDULE_NOW)/BATCH_CONTROL_SIZE)*BATCH_CONTROL_SIZE,'1970-01-01')
    END BATCH_POSSIBLE
  FROM (
	SELECT BATCH_CONTROL_COLUMN,
      BATCH_CONTROL_SIZE,
      BATCH_PROCESSED,
      BATCH_PROCESSING,
      BATCH_SCHEDULE_TYPE,
      BATCH_SCHEDULE_LAST,
      CURRENT_TIMESTAMP() BATCH_SCHEDULE_NOW
	FROM DATA_AGGREGATION_TARGETS
	WHERE TARGET_DATA = :1
	)
  )
WHERE BATCH_PROCESSING IS NULL
OR DATEDIFF(MINUTE, BATCH_SCHEDULE_LAST, BATCH_SCHEDULE_CURRENT) > 120;`;

var targetStmt = snowflake.createStatement({
	sqlText: targetQuery,
	binds: [TARGET_DATA]
});

var target = targetStmt.execute();

if (target.next()) {
	batchControlColumn = target.getColumnValue(1);
	batchControlSize = target.getColumnValue(2);
	batchScheduleType = target.getColumnValue(3);
	chunkLoopTag = target.getColumnValue(4);
	chunkLoopEnd = target.getColumnValue(5);
	batchScheduleCurrent = target.getColumnValue(6);
}
else {
	return '\n\n-- Skip this schedule as previous schedule has not done yet!\n'
}

/**
	* Construct the SQL syntax elements parser object.
	* @enum {string}
	*/
var astParser = {
	batchControlColumn: batchControlColumn,
	targetData: TARGET_DATA
};

/**
 * Initialize the batch exclusion control context
 */
var contextQuery = `UPDATE DATA_AGGREGATION_TARGETS \n `
	+ `SET BATCH_PROCESSING = :2, \n\t `
	+ `BATCH_SCHEDULE_LAST = :3 \n`
	+ `WHERE TARGET_DATA = :1;`;
var contextStmt = snowflake.createStatement({
	sqlText: contextQuery,
	binds: [
		TARGET_DATA, 
		chunkLoopEnd, 
		batchScheduleCurrent
	]
});

if (!SCRIPT_ONLY) { contextStmt.execute(); }

/**
 * Loop and call the date_poplate SP for each batch
 */
while (chunkLoopTag <= chunkLoopEnd) {
	var lengthOfISO = (new RegExp('HOUR|MINUTE|SECOND')).test(batchScheduleType) ? 19 : 10;
	var dateTimeISO = dateAdd(batchScheduleType, 0, chunkLoopTag).toISOString().substring(0, lengthOfISO);
	var chunkEndISO = dateAdd(batchScheduleType, batchControlSize, chunkLoopTag).toISOString().substring(0, lengthOfISO);

	/**
	 * Update the progress indicator
	 */
	var contextQuery = `UPDATE DATA_AGGREGATION_TARGETS \n `
		+ `SET BATCH_MICROCHUNK_CURRENT = :2 \n `
		+ `WHERE TARGET_DATA = :1;`;
	var contextStmt = snowflake.createStatement({
		sqlText: contextQuery,
		binds: [
			TARGET_DATA, 
			dateTimeISO
		]
	});
	if (!SCRIPT_ONLY) { contextStmt.execute(); }

	/**
	 * Parse the syntax to simplified non-standard AST
	 * then generate the final SQL statement from the it
	 */
	var deleteQueryTemplate = sqlDialect[templateName];
	var sqlAST = sqlToAst(deleteQueryTemplate, astParser);
	var deleteQuery = astToSql(deleteQueryTemplate, sqlAST);

	/**
	 * Generate the log information for the executed SQL
	 */
	var deleteScheduled = deleteQuery
		.replace(/:2/g, '\'' + chunkEndISO + '\'')
		.replace(/:1/g, '\'' + dateTimeISO + '\'');

	var callQuery = `CALL DATA_AGGREGATOR (:1, :2, :3, :4, :5);\n`;
	var callScheduled = callQuery
		.replace(/:1/g, '\'' + TARGET_DATA + '\'')
		.replace(/:2/g, SCRIPT_ONLY.toString())
		.replace(/:3/g, LOG_DETAILS.toString())
		.replace(/:4/g, NON_ENABLED.toString())
		.replace(/:5/g, '\'' + dateTimeISO + '\'');

	/**
	 * Process a chunk of data
	 */
	if (!SCRIPT_ONLY) {
		var logQuery = 'INSERT INTO DATA_AGGREGATION_LOGGING(EVENT_TARGET, EVENT_SOURCE, EVENT_STATUS, EVENT_STATE, EVENT_QUERY) \n'
				+ 'VALUES(:1, :2, :3, :4, :5)';

		/**
		 * Remove the existed chunk data and log SQL result
		 */
		try {
			var removalStmt = snowflake.createStatement({
				sqlText: deleteQuery,
				binds: [
					dateTimeISO, 
					chunkEndISO
				]
			});
			removalStmt.execute();
			callStatus = 'SUCCESS';
			callResult = '[INFO-1] Successfully deleted the existing data for reloading';
		}
		catch (err) {
			callStatus = 'FAILURE';
			callResult = '[FAIL] Failure to delete the existing data from target table => ' + err
		}
		finally {
			if (LOG_DETAILS || callStatus.startsWith('FAIL')) {
				var logStmt = snowflake.createStatement({
					sqlText: logQuery,
					binds: [
						TARGET_DATA, 
						'(*** EXISTED DATA REMOVAL ***)', 
						callStatus, 
						callResult, 
						deleteScheduled
					]
				});
				logStmt.execute()
			}
		}

		/**
		 * Log a chunk processor calling information before each chunk starts
		 */
		if (LOG_DETAILS) {
			var logStmt = snowflake.createStatement({
				sqlText: logQuery,
				binds: [
                    TARGET_DATA, 
                    '(*** CHUNK PROCESS CALLER ***)', 
                    '(START...)', 
                    '[INFO-2] Make the chunk processor call', 
                    callScheduled
				]
			});
			logStmt.execute()
		}

		/**
		 * Call chunk processor to generate the chunk data and log the result
		 */
		try {
			var callStmt = snowflake.createStatement({
				sqlText: callQuery,
				binds: [
					TARGET_DATA, 
					SCRIPT_ONLY.toString(), 
					LOG_DETAILS.toString(), 
					NON_ENABLED.toString(), 
					dateTimeISO
				]
			});
			callStmt.execute();
			callStatus = '(...DONE)';
			callResult = '[INFO-2] Successfully completed the batch load call';
		}
		catch (err) {
			callStatus = 'FAILURE';
			callResult = '[FAIL] Failure to complete the batch load call => ' + err
		}
		finally {
			if (LOG_DETAILS || callStatus.startsWith('FAIL')) {
				var logStmt = snowflake.createStatement({
					sqlText: logQuery,
					binds: [
						TARGET_DATA, 
						'(*** CHUNK PROCESS CALLER ***)', 
						callStatus, 
						callResult, 
						callScheduled
					]
				});
				logStmt.execute()
			}
		}
	}

	/**
	 * Generate the output script for manual execution check
	 */
	loopSegmenter = pageBreaker + '-'.repeat(65)
		+ `\n-- LOOP FRAME: ` + batchControlColumn + ` = ` + dateTimeISO
		+ `\n-- LOOP CHUNK: CHUNK_SIZE = ` + batchControlSize.toString() + ` ` + batchScheduleType + `(S) BY ` + batchControlColumn
		+ `\n` + '-'.repeat(65) + `\n`;
	loopScript = loopScript + loopSegmenter + deleteScheduled + callScheduled;
	pageBreaker = `\n\n`;

	chunkLoopTag = Date.parse(chunkEndISO);
}

/**
 * Clear the batch exclusion control context
 */
var contextQuery = `UPDATE DATA_AGGREGATION_TARGETS T \n`
	+ `SET BATCH_MICROCHUNK_CURRENT = NULL, BATCH_PROCESSING = NULL, BATCH_PROCESSED = S.SOURCE_READY_TIME \n`
	+ `FROM ( \n`
	+ `SELECT d.TARGET_DATA, MIN(COALESCE(s.SOURCE_READY_TIME, d.BATCH_PROCESSED)) SOURCE_READY_TIME \n`
	+ `FROM DATA_AGGREGATION_TARGETS d \n`
	+ `JOIN DATA_AGGREGATION_SOURCES s \n`
	+ `USING(TARGET_ID) \n`
	+ `WHERE s.SOURCE_ENABLED = True \n`
	+ `GROUP BY d.TARGET_DATA \n`
	+ `) S \n`
	+ `WHERE T.TARGET_DATA = S.TARGET_DATA AND T.TARGET_DATA = :1;`;
var contextStmt = snowflake.createStatement({
	sqlText: contextQuery,
	binds: [TARGET_DATA]
});

if (!SCRIPT_ONLY) { contextStmt.execute(); }

return loopScript;
$$;
-------------------------------------------------------
-- Data redeadiness detector procedue to update all source ready time
-------------------------------------------------------
-- DROP PROCEDURE DATA_DETECTOR(VARCHAR, BOOLEAN, BOOLEAN, BOOLEAN);
CREATE OR REPLACE PROCEDURE  DATA_DETECTOR (
	TARGET_DATA VARCHAR,
	SCRIPT_ONLY BOOLEAN,
	LOG_DETAILS BOOLEAN,
	NON_ENABLED BOOLEAN
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT STRICT
AS
$$
/**
 * Generate a simplified non-standard AST from a SQL syntax
 * @param {string} sqlSyntax, {object} astParser
 * @return {object>}
 */
function sqlToAst(sqlSyntax, astParser) {
    var sqlAst = {};
	var sqlRegex = /(?<=\<\b(?![\s|=|>|0-9]))(.*?)(?=\>)/g;
	var items = [...new Set(sqlSyntax.match(sqlRegex))].sort();
	for (var i = 0; i < items.length; i++) {
		sqlAst[items[i]] = astParser[items[i]] === undefined ? '/*' + items[i] + '*/' : astParser[items[i]];
	}
    return sqlAst;
}

/**
 * Generate a SQL statement from a simplified non-standard AST
 * @param {string} sqlSyntax, {object} sqlAst
 * @return {string>}
 */
function astToSql(sqlSyntax, sqlAst) {
    var astKeys = Object.keys(sqlAst);
	var sqlStatement = sqlSyntax;
	for (var i = 0; i < astKeys.length; i++) {
        var key = astKeys[i];
		sqlStatement = sqlStatement.split('<' + key + '>').join(sqlAst[key]);
	}
	return sqlStatement;
}

/**
 * Define the SQL templates of the dialect of snowflake.
 * @enum {string}
 */
var sqlDialect = {};
sqlDialect.update = `UPDATE DATA_AGGREGATION_SOURCES S \n`
	+ `SET S.SOURCE_CHECK_TIME = CURRENT_TIMESTAMP, \n`
	+ `    S.SOURCE_READY_TIME = <detectFunction> \n`
	+ `FROM DATA_AGGREGATION_TARGETS T \n`
	+ `WHERE S.TARGET_ID = T.TARGET_ID \n`
	+ `AND T.TARGET_DATA = :1 \n`;
	+ `AND S.SOURCE_DATA = :2;`;

/**
 * Get chunk processing config data from control database
 * @enum {object}
 */
var sqlScript = '', pageBreaker = '', templateName = 'update';
var sourceQuery = `SELECT
	  d.TARGET_DATA,
	  d.BATCH_CONTROL_SIZE,
	  d.BATCH_SCHEDULE_TYPE,
	  d.BATCH_PROCESSED,
	  s.SOURCE_LABEL,
	  s.SOURCE_DATA,
	  s.SOURCE_CHECK_QUERY
  FROM DATA_AGGREGATION_TARGETS d
  JOIN DATA_AGGREGATION_SOURCES s
  USING(TARGET_ID)
  WHERE d.TARGET_DATA = :1
	AND s.SOURCE_ENABLED != :2;`;

var sourceStmt = snowflake.createStatement({
	sqlText: sourceQuery,
	binds: [
		TARGET_DATA, 
		NON_ENABLED
	]
});

var sources = sourceStmt.execute();

/**
 * Loop and process all enabled sources
 */
while (sources.next()) {
	var targetData = sources.getColumnValue(1);
	var batchControlSize = sources.getColumnValue(2);
	var batchScheduleType = sources.getColumnValue(3);
	var batchProcessedTime = sources.getColumnValue(4);
	var sourceLabel = sources.getColumnValue(5);
	var sourceData = sources.getColumnValue(6);
	var sourceCheckQuery = sources.getColumnValue(7);
	var sourceTitle = '',
		sqlExecuted = '',
		sqlStatus = '',
		sqlResult = '(SP call parameter script_only is presented true)';

	/**
	 * Construct the SQL syntax elements parser object.
	 * @enum {string}
	 */
	var astParser = {};
	astParser.detectFunction = `DATEADD('QUARTER', -1, DATE_TRUNC('QUARTER', CURRENT_DATE())) -1`;

	/**
	 * Parse the syntax to simplified non-standard AST
	 * then generate the final SQL statement from the it
	 */
	var detectQueryTemplate = sourceCheckQuery ? sourceCheckQuery : sqlDialect[templateName];
	var sqlAST = sqlToAst(detectQueryTemplate, astParser);
	var detectQuery = astToSql(detectQueryTemplate, sqlAST);

	/**
	 * Generate the log information for the executed SQL
	 */
	sqlExecuted = detectQuery
		.replace(/:1/g, "'" + targetData + "'")
		.replace(/:2/g, "'" + sourceData + "'");

	/**
	 * Execute the SQL statement against the warehouse database 
	 * then log the execution information accordingly
	 */
	if (!SCRIPT_ONLY) {
		try {
			var loadStmt = snowflake.createStatement({
				sqlText: detectQuery,
				binds: [
					targetData, 
					sourceData
				]
			});
			loadStmt.execute();
			sqlStatus = 'SUCCESS';
			sqlResult = '[INFO-1] Successfully update the source readiness time'
		}
		catch (err) {
			sqlStatus = 'FAILURE';
			sqlResult = '[FAIL] Failure to update the source readiness time => ' + err
		}
		finally {
			if (LOG_DETAILS || sqlStatus.startsWith('FAIL')) {
				var logQuery = 'INSERT INTO DATA_AGGREGATION_LOGGING(EVENT_TARGET, EVENT_SOURCE, EVENT_STATUS, EVENT_STATE, EVENT_QUERY) \n'
						+  'VALUES(:1, :2, :3, :4, :5)';
				var logStmt = snowflake.createStatement({
					sqlText: logQuery,
					binds: [
						targetData, 
						sourceData, 
						sqlStatus, 
						sqlResult, 
						sqlExecuted
					]
				});
				logStmt.execute()
			}
		}
	}

	/**
	 * Generate the output script for manual execution check
	 */
	sourceTitle = pageBreaker + '-'.repeat(65)
		+ `\n-- SOURCE_LABEL: ` + sourceLabel
		+ `\n-- SOURCE_DATA: ` + sourceData.replace('DATAMART.BUYSIDE_NETWORK.', '').replace('DATAMART.SELLSIDE_NETWORK.', '')
		+ `\n-- SOURCE STATE: ` + sqlResult
		+ `\n` + '-'.repeat(65) + `\n`;
	sqlScript = sqlScript + sourceTitle + sqlExecuted;
	pageBreaker = `\n\n`;
}

/**
 * Return the full script
 * @return detectQuery; 
 */
return sqlScript;
$$;
