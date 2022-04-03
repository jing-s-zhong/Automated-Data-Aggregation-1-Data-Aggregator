# Data Process Architecture (1): Data Aggregator

There are three prerequites to setup the data aggregation solution.
(1) A snowflake access account;
(2) A existing snowflake database;
(3) A snowSQL command-line client.

## I. Setup the development environment

Create a working folder at the local computer, for Windows OS command-line

```
C:
mkdir GitHub
cd GitHub

```

Configure the snowSQL client as a text file in the local working folder

```
C:\GitHub\config\.snowsql\config

```


Clone the solution scripts to a local repository 

```
git clone https://github.com/Openmail/BusinessIntelligence.git

```


Go to the folder of the solution scripts, the Windows OS example is

```
cd C:\GitHub\BusinessIntelligence\S1-Data-Aggregation-1-Data-Aggregator\scripts

```


## II. Deploy the solution to the stage or production

Suppose you have a snowflake database BI_TEST is created, and you want to setup the solution in schema _CONTROL_LOGIC

### Deploy the solution

Deploy the solution to snowflake DB schema "BI_TEST"."_CONTROL_LOGIC"

```
schedule1_deploy.bat BI_TEST _CONTROL_LOGIC
```


### Test the solution

Test the new deployed solution at DB schema "BI_TEST"."_CONTROL_LOGIC"

```
schedule2_test.bat BI_TEST _CONTROL_LOGIC
```


### Clear the test data

Clean up the test data from DB schema "BI_TEST"."_CONTROL_LOGIC"

```
schedule1_empty.bat BI_TEST _CONTROL_LOGIC
```


### Remove the solution

Remove the solution from snowflake DB schema "BI_TEST"."_CONTROL_LOGIC"

```
schedule5_remove.bat BI_TEST _CONTROL_LOGIC
```


### Upgrade the solution

Upgrade the solution from an old version at DB schema "BI_TEST"."_CONTROL_LOGIC"

```
schedule6_upgrade.bat BI_TEST _CONTROL_LOGIC
```


### Degrade the solution

Degrade the solution to the old version before upgrade at DB schema "BI_TEST"."_CONTROL_LOGIC"

```
schedule7_degrade.bat BI_TEST _CONTROL_LOGIC
```


## III. Setup the solution for data processing


### Create or select a target table to accept the summary data


### Configure the target table in the solution


### Configure the source data which will be aggregated


### Make a manual test processing


### Make an automation full processing


### Setup snow tasks for schedule automation
