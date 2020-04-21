import time
import pickle
from pathlib import Path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.errors import HttpError
from aircal.export import INSERT_ACTION, UPDATE_ACTION, DELETE_ACTION


SCOPES = ['https://www.googleapis.com/auth/calendar']
TITLE_PREFIX = 'DAG:'


class GCalClient(object):

    def __init__(self, calendar_id, creds_dir, logger, max_results=2000):
        creds = self._auth(creds_dir)
        self.calendar_id = calendar_id
        self.service = build('calendar', 'v3', credentials=creds)
        self.max_results = max_results
        self.logger = logger

    def _auth(self, creds_dir):
        creds = None
        token_path = creds_dir / 'token.pickle'
        creds_path = creds_dir / 'credentials.json'
        
        # The file token.pickle stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.         
        if token_path.exists():
            with open(token_path, 'rb') as token:
                creds = pickle.load(token)
        
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(creds_path, SCOPES)
                creds = flow.run_local_server(port=0)
            with open(token_path, 'wb') as token:
                pickle.dump(creds, token)
        
        return creds

    def create_event(self, dag_id, start_date, end_date):
        event = {
            'summary': 'DAG: %s' % dag_id,
            'start': {
                'dateTime': start_date.strftime('%Y-%m-%dT%H:%M:0'),
                'timeZone': 'Etc/UTC',
            },
            'end': {
                'dateTime': end_date.strftime('%Y-%m-%dT%H:%M:0'),
                'timeZone': 'Etc/UTC',
            }
        }
        event = self.service.events().insert(calendarId=self.calendar_id, body=event).execute()
        return event['status']

    def delete_event(self, event_id):
        self.service.events().delete(calendarId=self.calendar_id, eventId=event_id).execute()
        return 'deleted'

    def update_event(self, event_id, dag_id, start_date, end_date):
        event = {
            'summary': 'DAG: %s' % dag_id,
            'start': {
                'dateTime': start_date.strftime('%Y-%m-%dT%H:%M:0'),
                'timeZone': 'Etc/UTC',
            },
            'end': {
                'dateTime': end_date.strftime('%Y-%m-%dT%H:%M:0'),
                'timeZone': 'Etc/UTC',
            }
        }
        event = self.service.events().update(calendarId=self.calendar_id, eventId=event_id, body=event).execute()
        return event['status']

    def do_sync(self, v):
        for i in range(3):
            try:
                if v.action == INSERT_ACTION:
                    self.create_event(v.dag_id, v.start_date, v.end_date)
                elif v.action == DELETE_ACTION:
                    self.delete_event(v.event_id)
                elif v.action == UPDATE_ACTION:
                    self.update_event(v.event_id, v.dag_id, v.start_date, v.end_date)
                else:
                    raise Exception('action not supported')
            except HttpError as ex:
                print(ex)
                self.logger.error('HTTP exception occured, retrying')
                time.sleep(10**(i+1))
            else:
                return 0
        return 1

    def get_events(self, base_date):
        events_result = self.service.events().list(
            calendarId=self.calendar_id, maxResults=self.max_results,
            timeMin=base_date, singleEvents=True, orderBy='startTime'
        ).execute()
        events = events_result.get('items', [])
        if len(events) == self.max_results:
            raise Exception((
                '# of retrieved events equals to max results. Some events might be ignored. '
                'Consider increasing max_results parameter or decrease n_horizon_days.'))
        elig_events = [v for v in events if v.get('summary', '').startswith(TITLE_PREFIX)]
        for event in elig_events:
            event['summary'] = event['summary'].replace(TITLE_PREFIX, '').strip()
        return elig_events
