import os
import sys
import argparse
import logging
from pathlib import Path
from datetime import timedelta
from sqlalchemy import create_engine
from aircal.events import DagRunEventsExtractor
from aircal.dao.airflow import AirflowDb
from aircal.dao.gcal import GCalClient
from aircal.export import GCalExporter


logger = logging.getLogger('aircal')
logger.setLevel(logging.INFO)
cli_handler = logging.StreamHandler()
cli_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
cli_handler.setFormatter(cli_format)
logger.addHandler(cli_handler)


def do_continue(df_events):
    if df_events.shape[0] <= 500:
        return
    logger.info('# events to manage is high: %d' % df_events.shape[0])
    logger.info('You might want to consider filtering them to reduce clutter.')
    yn = input('Are you sure you want to export all of them (y/n): ')
    if yn != 'y':
        logger.info('Too many events, exiting.')
        sys.exit(1)


def main(args):
    logger.info('Extracting dag run events.')    
    airflow_db = AirflowDb(create_engine(args.sqlalchemy_conn_string))    
    extractor = DagRunEventsExtractor(airflow_db, n_horizon_days=args.n_horizon_days, n_last_runs=args.n_last_runs)

    # extract future all future DAG runs for the given horizon
    df_events = extractor.get_events_df()
    # filter out the ones that are of no interest of you
    # in this case we only keep the ones that are running more than x minutes
    df_events = df_events[df_events.mean_exec_time > timedelta(minutes=args.min_dag_exec_time)]

    do_continue(df_events)

    logger.info('Syncing to GCal.')
    gcal_client = GCalClient(calendar_id=args.calendar_id, creds_dir=Path(args.creds_path), logger=logger)
    exporter = GCalExporter(gcal_client, df_events)
    # sync the calendar state with the freshly retrieved future DAG runs
    df_updated = exporter.sync_events()

    if not df_updated.empty:
        # save the data frame for inspection
        df_updated.to_csv('event_ops.csv', index=False)
        logger.info('Sync complete.')
    else:
        logger.info('Nothing to sync.')


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--sqlalchemy-conn-string', required=True,
        help='sql connection needed to read dag and dag_run tables in airflow DB')
    parser.add_argument('--calendar-id', required=True,
        help='calendar where the DAG run events will be created')
    parser.add_argument('--creds-path', default=os.getcwd(),
        help='place to store credentials.json; also used to store a token received from the Google API')
    parser.add_argument('--n-horizon-days', type=int, default=10,
        help='how many days in advance we want the events to be created')
    parser.add_argument('--n-last-runs', type=int, default=5,
        help='number of recent DAG runs to estimate its execution time')
    parser.add_argument('--min-dag-exec-time', type=int, default=0,
        help='min execution time of a DAG to export to the calendar')
    args = parser.parse_args()
    
    main(args)