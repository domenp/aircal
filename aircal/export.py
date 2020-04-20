import numpy as np
import pandas as pd
from datetime import datetime


INSERT_ACTION = 'insert'
UPDATE_ACTION = 'update'
DELETE_ACTION = 'delete'


class GCalExporter(object):

    def __init__(self, gcal):
        self.gcal = gcal

    def _get_gcal_events(self):
        now = datetime.utcnow().isoformat() + 'Z'
        events = self.gcal.get_events(now)
        df_gcal = pd.DataFrame(data={
            'dag_id': [v['summary'] for v in events],
            'start_date': [v['start']['dateTime'] for v in events],
            'end_date': [v['end']['dateTime'] for v in events],
            'event_id': [v['id'] for v in events],
            'source': 'gcal'
        })

        if df_gcal.empty:
            return df_gcal

        df_gcal.start_date = pd.to_datetime(df_gcal.start_date).dt.tz_convert('UTC').dt.tz_localize(None)
        df_gcal.end_date = pd.to_datetime(df_gcal.end_date).dt.tz_convert('UTC').dt.tz_localize(None)

        return df_gcal
    
    def _determine_overlap(self, df_elig_events, df_gcal):
        df_new = df_elig_events.copy()
        df_new = df_new.set_index([df_new.dag_id, df_new.start_date])
        df_new = df_new[['end_date']].rename(columns={'end_date': 'end_date_events'})
        
        df_cur = df_gcal.copy()
        df_cur = df_cur.set_index([df_cur.dag_id, df_cur.start_date])
        df_cur = df_cur[['end_date', 'event_id']].rename(columns={'end_date': 'end_date_gcal'})
        
        return pd.concat([df_new, df_cur], axis=1).reset_index()
    
    def mark_for_sync(self, df_events, exec_time_diff_tol_seconds=360):
        
        df_elig_events = df_events.copy()
        
        df_gcal = self._get_gcal_events()
        df_ov = self._determine_overlap(df_elig_events, df_gcal)
        
        df_to_insert = df_ov[df_ov.end_date_gcal.isna()].copy()
        df_to_insert['action'] = INSERT_ACTION

        df_to_delete = df_ov[df_ov.end_date_events.isna()].copy()
        df_to_delete['action'] = DELETE_ACTION

        dfs = [df_to_insert, df_to_delete]

        if df_ov[~df_ov.end_date_events.isna()].shape[0] and df_ov[~df_ov.end_date_gcal.isna()].shape[0]:
            df_to_update = df_ov[(df_ov.end_date_events - df_ov.end_date_gcal).dt.seconds > exec_time_diff_tol_seconds].copy()
            df_to_update['action'] = UPDATE_ACTION
            dfs.append(df_to_update)

        df_to_sync = pd.concat(dfs)

        if df_to_sync.empty:
            return df_to_sync

        df_to_sync['end_date'] = df_to_sync.apply(
            lambda x: x.end_date_events if x.end_date_events else x.end_date_gcal, axis=1)

        return df_to_sync.drop(columns=['end_date_events', 'end_date_gcal'])

    def sync(self, df_to_sync):
        if df_to_sync.empty:
            return df_to_sync

        df_to_sync['error'] = df_to_sync.apply(self.gcal.do_sync, axis=1)
        return df_to_sync