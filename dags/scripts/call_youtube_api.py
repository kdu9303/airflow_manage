import os
import json
import os.path
import pickle
# import httplib2
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
# from oauth2client.client import GoogleCredentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from airflow.models import Variable

FILE_PATH = '/opt/airflow/plugins/'
CLIENT_SECRETS_FILE = FILE_PATH + "youtube_credentials_installed.json"
SCOPES = [
    'https://www.googleapis.com/auth/youtube',
    'https://www.googleapis.com/auth/youtube.force-ssl',
    'https://www.googleapis.com/auth/youtube.readonly'
]


def get_authenticated_service_using_api() -> object:
    api_key = Variable.get("YOUTUBE_API_KEY")
    return build('youtube', 'v3', developerKey=api_key, cache_discovery=False)


# def get_authenticated_service():

    if os.path.isfile(f"{FILE_PATH}refreshed_credentials.json"):
        with open(f"{FILE_PATH}refreshed_credentials.json", 'r') as f:
            creds_data = json.load(f)
        creds = Credentials(
            # access_token=creds_data["token"],
            token=creds_data["token"],
            client_id=creds_data["client_id"],
            client_secret=creds_data["client_secret"],
            token_uri="https://accounts.google.com/o/oauth2/token",
            refresh_token=creds_data["refresh_token"],
            # token_expiry=None
            expiry=None
            # user_agent='MyAgent/1.0'
        )
        # if creds is None or creds.invalid:
        if not creds or not creds.valid:
            # http = creds.authorize(httplib2.Http())
            creds.refresh(Request())

    else:
        flow = InstalledAppFlow.\
            from_client_secrets_file(CLIENT_SECRETS_FILE, SCOPES)
        creds = flow.run_console()
        creds_data = {
            'token': creds.token,
            'refresh_token': creds.refresh_token,
            'token_uri': creds.token_uri,
            'client_id': creds.client_id,
            'client_secret': creds.client_secret,
            'scopes': creds.scopes
        }
        print(creds_data)
        with open(f"{FILE_PATH}refreshed_credentials.json", 'w') as outfile:
            json.dump(creds_data, outfile)

    return build('youtube', 'v3', credentials=creds, cache_discovery=False)


def get_authenticated_service():

    creds = None

    if os.path.isfile(f"{FILE_PATH}token.pickle"):
        with open(f"{FILE_PATH}token.pickle", 'rb') as token:
            print("Credential 정보를 파일로부터 받아오고 있습니다.")
            # print(f"토큰 위치:{os.path.realpath('token.pickle')}")
            creds = pickle.load(token)

    if not creds or not creds.valid:

        if creds and creds.expired and creds.refresh_token:
            print("토큰 정보를 가져오고 있습니다.")
            creds.refresh(Request())

        else:
            flow = InstalledAppFlow.\
                from_client_secrets_file(CLIENT_SECRETS_FILE, SCOPES)

            creds = flow.run_console()

        with open(f"{FILE_PATH}token.pickle", 'wb') as token:
            print("Creddential 정보를 token으로 저장합니다.")
            pickle.dump(creds, token)

    return build('youtube', 'v3', credentials=creds, cache_discovery=False)


if __name__ == '__main__':
    # 최초 인증을 위해 로컬로 한번 실행한다.
    # os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
    get_authenticated_service()
