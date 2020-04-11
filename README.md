# Aircal

Aircal is a library that exports future DAG runs as events to Google Calendar.

Having DAG run as events in the calendar may help you:
- visualize the utilization of your airflow workers to better spread your jobs
- determine when a certain DAG should be finished to monitor the service.

The library will also observe the changes to your DAGs and synchronize it with the calendar:
- add runs for the freshly added DAGs
- change start and/or end time when an existing DAG changes the schedule (or the execution time changes significantly)
- delete run events when a DAG is removed (or paused)

Tip: run the sync script regularly, perhaps, with you know, Airflow :)

The library only support DAG schedules that use the standard cron syntax. The rest will be ignored (with a warning).

**Warning: This is an alpha stage software. Expect occassional bugs and rough edges (PR welcome).**

## Installation & setup

There's no package available (on PyPi) at the moment so you need to clone this repo and install it locally with the command below:

```
pip install -e .
```

Google API credentials are required to create events in the calendar. You can obtain them [here](https://console.developers.google.com/apis/credentials). Store `credentials.json` into a directory accessible by your code.

**The library is modifying and deleting calendar events. I highly recommend creating a new calendar to be used by this software:** "add calendar" -> "create new calendar" in Google calendar settings.

## Usage

A sample sync script you can run on a regular interval:

```python
from sqlalchemy import create_engine
from aircal.events import DagRunEventsExtractor
from aircal.dao.airflow import AirflowDb
from aircal.dao.gcal import GCalClient
from aircal.export import GCalExporter

logger = ...

# we'll use sqlalchemy to connect to Airflow DB and read DAG metadata (dag and dag_run tables)
sqlalchemy_conn_string = '<conn_string>'
# calendar in which DAG run events will be created
calendar_id = '<calendar_id>'
# directory to put credentials.json
# the library will reuse this directory to store a token received from the Google API
creds_path = '/path/to/creds_dir'
# how many days in advance we want the events to be created
n_horizon_days=10
# number of recent DAG runs to estimate its execution time
n_last_runs = 5
# ignore frequent DAGs that run more than x time per day to avoid cluttering the calendar
max_events_per_day = 10


if __name__ == '__main__':

    logger.info('Extracting dag run events.')
    airflow_db = AirflowDb(create_engine(sqlalchemy_conn_string))
    extractor = DagRunEventsExtractor(airflow_db, n_horizon_days=n_horizon_days, n_last_runs=5)
    df_events = extractor.get_events_df() # extract future DAG runs: dag_id, start_date, execution time

    logger.info('Syncing to GCal.')
    gcal_client = GCalClient(calendar_id=calendar_id, creds_dir=Path(creds_path), logger=logger)
    exporter = GCalExporter(gcal_client, df_events, n_horizon_days=n_horizon_days,
        max_events_per_day=max_events_per_day)
    df_updated = exporter.sync_events()

    if not df_updated.empty:
        logger.info('Sync complete.')
        logger.info('The following events have been inserted, updated, or deleted:\n%s' % df_updated)
    else:
        logger.info('Nothing to sync.')
```
