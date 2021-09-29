from googleapiclient.discovery import build  
from google.oauth2 import service_account

# timeout: The read operation timed out 에러 발생시...
import socket
socket.setdefaulttimeout(300) # 5 minutes


SCOPES = ['https://www.googleapis.com/auth/spreadsheets']  

# IAM 및 관리자 -> 서비스 계정 -> JSON 키 새로 발급
credentials = service_account.Credentials.from_service_account_file(
    '/home/ubuntu/keyfiles/google_service_account_key.json',
     scopes=SCOPES)

service = build('sheets', 'v4', credentials=credentials)
sheet = service.spreadsheets()



SPREADSHEET_ID = ["1oct2vlwgSm5LyItMW8myVm4Iv9839RHiTm9ro8kdr70","1VsFdlwavKzKhUVZard93W28_Z2PiKPxZSQE8sWG89YA"]

for doc in SPREADSHEET_ID:
        sheet_metadata = sheet.get(spreadsheetId=doc).execute()  # 구글스프레드시트 전체 정보
        file_name = sheet_metadata.get('properties',{}).get('title','')
        print(file_name)

