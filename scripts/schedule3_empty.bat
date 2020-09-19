@echo off
rem ====================================================
rem Schedule-3: Empty the DATA_AGGREGATOR
rem ----------------------------------------------------
rem Example: schedule3_empty.bat BI_TEST DATA_AGGREGATOR
rem ====================================================

if [%1]==[] goto missDb
if [%2]==[] goto missSchema

echo Emptying the test data of the data aggregator in %1.%2
snowsql ^
--config ..\..\config\.snowsql\config ^
-f .\schedule3_empty.sql ^
-o exit_on_error=true ^
-o quiet=true ^
-o friendly=true ^
-D db_name=%1 ^
-D sc_name=%2

if %ERRORLEVEL% neq 0 goto errorHandler

echo The test data of the data aggregator in %1.%2 is emptied
goto done

:missDb
echo First argument for DB name is missing!
goto example

:missSchema
echo Second argument for SCHEMA name is missing!

:example
echo Example: schedule3_empty.bat BI_TEST _CONTROL_LOGIC
goto done

:errorHandler
echo The data aggregator is failed to empty the test data

:done
echo(

