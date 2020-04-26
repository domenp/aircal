import pytest
import pandas as pd
from datetime import timedelta
from aircal.events import DagRunEventsExtractor


DEF_NUM_EVENTS = 250


class AirflowDbDefaultMock:

    def load_dag_metadata(self):
        return pd.DataFrame(data={
            'dag_id': ['test_dag', 'foobar'],
            'schedule_interval': ['5 10 * * *', '5 * * * *']
        })

    def load_dag_run_metadata(self):
        return pd.DataFrame(data={
            'dag_id': ['test_dag', 'foobar'],
            'start_date': ['2020-04-5 14:44:03.5', '2020-04-5 18:44:03.5'],
            'end_date': ['2020-04-5 15:44:03.5', '2020-04-5 20:44:03.5']
        })


def get_events():
    airflow_db = AirflowDbDefaultMock()
    extractor = DagRunEventsExtractor(airflow_db)
    return extractor.get_future_dag_runs(n_horizon_days=10)


def test_all_events_present():
    df_events = get_events()
    assert df_events.shape[0] == DEF_NUM_EVENTS


def test_all_essential_columns_not_na():
    df_events = get_events()
    assert df_events[~df_events[
            ['dag_id', 'schedule_interval', 'mean_exec_time', 'start_date', 'end_date']
        ].isna()].shape[0] == DEF_NUM_EVENTS
    

def test_mean_exec_time_est():
    df_events = get_events()
    assert all(df_events[df_events.dag_id == 'test_dag'].mean_exec_time == timedelta(hours=1))
