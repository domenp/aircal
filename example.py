import os
import sys
import argparse
import logging
from pathlib import Path
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


def main(args):
    logger.info('Extracting dag run events.')    
    airflow_db = AirflowDb(create_engine(args.sqlalchemy_conn_string))    
    extractor = DagRunEventsExtractor(airflow_db, n_horizon_days=args.n_horizon_days, n_last_runs=args.n_last_runs)
    df_events = extractor.get_events_df() # extract future DAG runs: dag_id, start_date, execution time

    """Skip events that occur too frequently not to overload the calendar."""
    df_counts = df_events.dag_id.value_counts().reset_index()
    df_counts = df_counts.rename(columns={'index': 'dag_id', 'dag_id': 'counts'})
    df_counts = df_counts[df_counts.counts < args.n_horizon_days * args.max_events_per_day]
    df_events = df_events.merge(df_counts, on='dag_id').drop(columns=['counts'])

    logger.info('Syncing to GCal.')
    gcal_client = GCalClient(calendar_id=args.calendar_id, creds_dir=Path(args.creds_path), logger=logger)
    exporter = GCalExporter(gcal_client, df_events)
    df_updated = exporter.sync_events()

    if not df_updated.empty:
        logger.info('Sync complete.')
        logger.info('The following events have been inserted, updated, or deleted:\n%s' % df_updated)
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
    parser.add_argument('--max-events-per-day', type=int, default=10,
        help='ignore DAGs that run more than x time per day to avoid cluttering the calendar')
    args = parser.parse_args()
    
    main(args)