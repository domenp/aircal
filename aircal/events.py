import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from croniter import croniter, CroniterBadCronError


class DagRunEventsExtractor(object):

    def __init__(self, airflow_db, base_date=datetime.now(), n_last_runs=5):
        self.airflow_db = airflow_db
        self.base_date = base_date
        self.n_last_runs = n_last_runs

    def _estimate_dag_exec_time(self):
        """
        Estimate execution time of the future DAG run.

        Takes an average of last 5 runs by default.
        """
        df_dag_run = self.airflow_db.load_dag_run_metadata()
        df_dag_run.start_date = pd.to_datetime(df_dag_run.start_date)
        df_dag_run.end_date = pd.to_datetime(df_dag_run.end_date)
        df_dag_run['exec_time'] = df_dag_run.end_date - df_dag_run.start_date

        df_et = df_dag_run.groupby('dag_id').\
            apply(lambda x: x.nlargest(self.n_last_runs, 'start_date')).reset_index(drop=True)
        df_met = df_et.groupby('dag_id')['exec_time'].agg(['sum', 'size'])
        df_met['mean_exec_time'] = df_met['sum'] / df_met['size']
        return df_met.drop(columns=['sum', 'size'])


    def _next_dag_runs(self, pattern, end_date):
        """
        Returns future DAG runs starting from the base date and to the end date.

        If the cron pattern is not recognized an empty list is returned.
        """
        if not pattern:
            return []

        pattern = pattern.strip('"')
        try:
            c = croniter(pattern, self.base_date)
        except CroniterBadCronError:
            return []

        dates = []
        while True:
            next_date = c.get_next(datetime)
            if next_date > end_date:
                break
            dates.append(next_date)
        return dates

    def get_future_dag_runs(self, n_horizon_days=30):
        """Returns data frame containing all upcoming (relative to the base date) DAG runs.

        Parameters
        ----------
        n_horizon_days : int

        Returns
        -------
        pandas.DataFrame
            Data frame containing columns: dag_id, start_date, end_date.
        """
        df_dag = self.airflow_db.load_dag_metadata()
        df_exec_time = self._estimate_dag_exec_time()
        df = df_dag.merge(df_exec_time, on='dag_id', how='left')

        # generate events from start time to the end of horizon
        date_end = self.base_date + timedelta(n_horizon_days)
        df['next_runs'] = df.apply(lambda v: self._next_dag_runs(v.schedule_interval, date_end), axis=1)

        # skip entries with no next runs
        # TODO: warn if no runs are scheduled
        df_ed = df[df.next_runs.str.len() != 0]

        df_events = df_ed.explode('next_runs')

        # if there's no execution time estimate default to one minute
        df_events.mean_exec_time = df_events.mean_exec_time.fillna(timedelta(minutes=1))

        df_events['start_date'] = df_events.next_runs
        df_events['end_date'] = df_events.next_runs + df_events.mean_exec_time

        return df_events.drop(columns=['next_runs'])
