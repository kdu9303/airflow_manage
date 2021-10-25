# -*- coding: utf-8 -*-
import logging
from googleapiclient.discovery import build
from google.oauth2 import service_account

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# json file path
FILE_PATH = '/opt/airflow/plugins/'

# test sheet
# SPREADSHEET_ID = ["1oct2vlwgSm5LyItMW8myVm4Iv9839RHiTm9ro8kdr70", "1VsFdlwavKzKhUVZard93W28_Z2PiKPxZSQE8sWG89YA"]
# SPREADSHEET_ID = ["1VsFdlwavKzKhUVZard93W28_Z2PiKPxZSQE8sWG89YA"]


#  시트 리스트에서 ID만 추출
def get_id(dept_values: str) -> list:
    dept_values_filtered = []
    for row in dept_values:
        try:
            dept_values_filtered.append(str(row).split('/')[5])
        except IndexError:
            # 빈칸 건너뛰는 용도
            pass
    return dept_values_filtered


def call_google_sheet() -> dict:

    # IAM 및 관리자 -> 서비스 계정 -> JSON 키 새로 발급
    credentials = service_account.Credentials.from_service_account_file(
        FILE_PATH + 'google_service_account_key.json',
        scopes=SCOPES)

    service = build('sheets', 'v4', credentials=credentials, cache_discovery=False)
    sheet = service.spreadsheets()

    logging.getLogger('googleapicliet.discovery_cache').setLevel(logging.ERROR)
    # 부서 전체목록 불러오기(실제 적용)
    dim_dept = sheet.values().get(spreadsheetId='14ps_HTgY1Y7ccW1zDMxl56AmorzZX-XmaTZYmyv4YLo',
                                  range='Sheet15!F2:F').execute()
    dept_values = dim_dept.get('values', [])

    SPREADSHEET_ID = get_id(dept_values)

    # (테스트시트용)
    # global SPREADSHEET_ID

    return sheet, SPREADSHEET_ID

