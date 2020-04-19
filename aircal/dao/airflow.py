import pandas as pd


class AirflowDb(object):

    def __init__(self, engine):
        self.engine = engine

    def load_dag_metadata(self):
        return pd.read_sql('SELECT dag_id, schedule_interval FROM dag WHERE is_paused = \'0\'', self.engine)

    def load_dag_run_metadata(self):
        df_dr = pd.read_sql('SELECT dag_id, start_date, end_date FROM dag_run', self.engine)
        return df_dr
