# -*- coding: utf-8 -*-
"""
Created on Thu Jun 20 10:05:15 2019

@author: Administrator
"""

from __future__ import print_function
import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

import time


# timeout: The read operation timed out 에러 발생시...
import socket
socket.setdefaulttimeout(300) # 5 minutes


# 구글API 토큰위치 지정(클라우드드라이브에 저장하면 안됨)
# os.chdir("/home/ubuntu/keyfiles/")
# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/ubuntu/keyfiles/credential.json'


# If modifying these scopes, delete the file token.pickle

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# The ID and range(스프레드시트는 항상 String값으로)
SPREADSHEET_ID = ["1oct2vlwgSm5LyItMW8myVm4Iv9839RHiTm9ro8kdr70","1VsFdlwavKzKhUVZard93W28_Z2PiKPxZSQE8sWG89YA"]
#RANGE = '!C2:E'  # 범위 지정
# Header는 데이터프레임에서 따로 지정할 것


#부서 체크용
dept_values = []



 # 링크에서 ID만 추출
def get_id(dept_values):
    dept_values_filtered = []
    for row in dept_values:
        try:
            dept_values_filtered.append(str(row).split('/')[5])
        except:
            pass
    return dept_values_filtered





def renameSpreadSheet(newName):

    requests = {
        "updateSpreadsheetProperties": {
            "properties": {
                "title": newName,
            },
            "fields": "title",
        }
    }
    body = {
        'requests': requests
    }
    return body


def main():
    """Shows basic usage of the Sheets API.
    Prints values from a spreadsheet.
    """
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                '/home/ubuntu/keyfiles/credentials.json', SCOPES)
            creds = flow.run_local_server(port=8888)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    service = build('sheets', 'v4', credentials=creds)

    # Call the Sheets API
    sheet = service.spreadsheets()


    #부서 id 불러오기
    # dim_dept = sheet.values().get(spreadsheetId='14ps_HTgY1Y7ccW1zDMxl56AmorzZX-XmaTZYmyv4YLo',
    #                               range='Sheet15!F2:F').execute()
    # global dept_values
    # dept_values = dim_dept.get('values', [])

    # SPREADSHEET_ID = get_id(dept_values)
    global SPREADSHEET_ID
    

    
    print("--------------작업 시작-------------------")
    
    # 시트 이름 변경
    cnt = 0
    for doc in SPREADSHEET_ID:
        sheet_metadata = sheet.get(spreadsheetId=doc).execute()  # 구글스프레드시트 전체 정보
        file_name = sheet_metadata.get('properties',{}).get('title','')
        print(file_name + " 작업 중...")
        
        
        if file_name.split('_')[0] == '메디':
            changed_name = file_name.replace('메디','인천')

            #스프레드시트 안에 시트 제목을 바꿀 경우    
#           sheets = sheet_metadata.get('sheets', '')  # 구글스프레드시트 안의 시트 정보
#           sheet_name = sheets[0].get('properties', {}).get('title', '')

            ####################################################                
            #### 시트 이름 변경작업(콜라보 시작시 분기 변경) #####
            sheet.batchUpdate(spreadsheetId=doc, body=renameSpreadSheet(changed_name)).execute()
            print(changed_name + "으로 변환 완료...")
        
            ####################################################
            ####################################################
        else:
            print(file_name + " Pass...")
            continue
        cnt += 1
        if cnt % 6 == 0:
            print("Sleeping...Process will continue in 100 seconds")
            time.sleep(100)
        



    print("------------------끄으으읕~!!-------------------------")


if __name__ == '__main__':
    main()

