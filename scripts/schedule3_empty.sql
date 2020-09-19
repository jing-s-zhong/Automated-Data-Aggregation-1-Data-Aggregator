!set variable_substitution=true;
!define ver=V20;
--
use database &{db_name};
use schema &{sc_name};
!set variable_substitution=false;
-------------------------------------------------------
-- Create a dummy aggreagtion table
-------------------------------------------------------
--
-- Remove registratered test source
--
DELETE FROM DATA_AGGREGATION_SOURCES;
--
-- Remove registratered test target
--
DELETE FROM DATA_AGGREGATION_TARGETS;
--
-- Remove the test loggings 
--
DELETE FROM DATA_AGGREGATION_LOGGING;
--
-- Drop the test data
--
DROP TABLE IF EXISTS _TEST_DATA_TARGET_1;
--
-- Drop the test data
--
DROP TABLE IF EXISTS _TEST_DATA_TARGET_2;
--
-- Drop the test data
--
DROP TABLE IF EXISTS _TEST_DATA_SOURCE_1;
--
-- Drop the test data
--
DROP TABLE IF EXISTS _TEST_DATA_SOURCE_2;
