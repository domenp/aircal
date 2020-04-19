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

See `example.py` for an example of the potential pipeline that can be run on the regular intervals.