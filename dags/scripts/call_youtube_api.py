import os
import json
import os.path
import pickle
import logging
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

# 로그 기록용
logger = logging.getLogger()


def get_authenticated_service_using_api() -> object:
    api_key = Variable.get("YOUTUBE_API_KEY")
    return build('youtube', 'v3', developerKey=api_key, cache_discovery=False)


def get_authenticated_service():

    try:

        creds = None

        if os.path.isfile(f"{FILE_PATH}token.pickle"):
            with open(f"{FILE_PATH}token.pickle", 'rb') as token:
                print("Credential 정보를 파일로부터 받아오고 있습니다.")
                # print(f"토큰 위치:{os.path.realpath('token.pickle')}")
                creds = pickle.load(token)

        else:

            # if not creds or not creds.valid:

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

    except Exception as e:
        logger.exception(f"{get_authenticated_service.__name__} --> {e}")
        raise


if __name__ == '__main__':
    # 최초 인증을 위해 로컬로 한번 실행한다.
    # os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
    print(get_authenticated_service())
