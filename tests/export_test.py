import pytest
import pandas as pd
from datetime import datetime
from aircal.export import GCalExporter, INSERT_ACTION, DELETE_ACTION, UPDATE_ACTION


NUM_EVENTS_TO_SYNC = 2

class GCalClientMock:

    def create_event(self, dag_id, start_date, end_date):
        return 'confirmed'

    def delete_event(self, event_id):
        return 'deleted'

    def update_event(self, event_id, dag_id, start_date, end_date):
        return 'confirmed'

    def do_sync(self, v):
        if v.action == INSERT_ACTION:
            self.create_event(v.dag_id, v.start_date, v.end_date)
        elif v.action == DELETE_ACTION:
            self.delete_event(v.event_id)
        elif v.action == UPDATE_ACTION:
            self.update_event(v.event_id, v.dag_id, v.start_date, v.end_date)
        else:
            raise Exception('action not supported')
        return 0

    def get_events(self, base_date):
        elig_events = [{
            'id': '1',
            'summary': 'foo',
            'start': {'dateTime': '2020-04-20T08:20:00Z'},
            'end': {'dateTime': '2020-04-20T08:22:00Z'}
        }]
        return elig_events


def test_sync():
    df_events = pd.DataFrame(data={
        'dag_id': ['foo', 'bar', 'baz'],
        'start_date': [datetime(2020, 4, 20, 8, 20), datetime(2020, 4, 20, 8, 21), datetime(2020, 4, 20, 8, 22)],
        'end_date': [datetime(2020, 4, 20, 8, 22), datetime(2020, 4, 20, 10, 20), datetime(2020, 4, 20, 16, 0)]
    })
    gcal_client = GCalClientMock()
    exporter = GCalExporter(gcal_client)
    df_to_sync = exporter.mark_for_sync(df_events)
    assert df_to_sync.shape[0] == NUM_EVENTS_TO_SYNC

    df_updated = exporter.sync(df_to_sync)
    assert df_updated.shape[0] == NUM_EVENTS_TO_SYNC
