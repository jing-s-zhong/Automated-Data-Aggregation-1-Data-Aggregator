!set variable_substitution=true;
use database &{db_name};
use schema &{sc_name};
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
-- Drop the test data
--
DROP TABLE TARGET_DATA_1;
--
-- Drop the test data
--
DROP TABLE TARGET_DATA_2;
--
-- Drop the test data
--
DROP TABLE SOURCE_DATA_1;
--
-- Drop the test data
--
DROP TABLE SOURCE_DATA_2;
