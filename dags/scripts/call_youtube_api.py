
# import pickle
# import os.path
from googleapiclient.discovery import build
from airflow.models import Variable

FILE_PATH = '/home/ubuntu/keyfiles/youtube_api_key.txt'
# CLIENT_SECRETS_FILE = FILE_PATH + "youtube_credentials_web_app.json"


def get_authenticated_service() -> object:

    api_key = Variable.get("YOUTUBE_API_KEY")
    return build('youtube', 'v3', developerKey=api_key)
