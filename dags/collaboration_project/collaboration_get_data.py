import logging
import time
import socket
import pandas as pd
import numpy as np
from datetime import datetime
# 부서 리스트 불러오기
from scripts.call_google_sheet import call_google_sheet

# timeout: The read operation timed out 에러 발생시...
socket.setdefaulttimeout(300)  # 5 minutes


# 콜라보 칼럼 커스터마이징
def df_column_etl(df):

    df['평가시기'] = f"{datetime.today().strftime('%Y')[2:]}.{(int(datetime.today().strftime('%m')) - 2) // 3 + 1}Q"
    df['평가기준일'] = pd.to_datetime(datetime.today().replace(day=1,
                                                          hour=0,
                                                          minute=0,
                                                          second=0,
                                                          microsecond=0))

    df['평가부서기구'] = df['평가부서_RAW'].str.slice(0, 2)

    df['점수'] = np.nan

    # 평가부서 extract
    for row in df.itertuples():
        try:
            if pd.isnull(df.loc[row.Index, '평가부서_RAW']) or \
               df.loc[row.Index, '평가부서_RAW'] == "":
                df.loc[row.Index, '평가부서_RAW'] = df.loc[row.Index - 1, '평가부서_RAW']
                df.loc[row.Index, '평가부서기구'] = df.loc[row.Index, '평가부서_RAW'].split('_')[:1] 
        except Exception:
            df.drop(row.Index, inplace=True, errors='ignore')

    for row in df.itertuples():
        try:
            if pd.isnull(df.loc[row.Index, '피평가부서_RAW']) or \
               df.loc[row.Index, '피평가부서_RAW'] == "":
                df.drop(row.Index, inplace=True, errors='ignore')
        except Exception as e:
            logging.info(e)

    # 친절,신뢰,소통및업무협조 칼럼이 비어있으면 해당 row 삭제
    df.dropna(subset=['친절', '신뢰', '소통및업무협조'], inplace=True)

    # 점수 칼럼 extract
    for row in df.itertuples(): 
        try:
            if not pd.isnull(df.loc[row.Index, '친절']) and \
               not pd.isnull(df.loc[row.Index, '신뢰']) and \
               not pd.isnull(df.loc[row.Index, '소통및업무협조']):

                df.loc[row.Index, '점수'] = round((float(df.loc[row.Index, '친절'])
                                                 + float(df.loc[row.Index, '신뢰'])
                                                 + float(df.loc[row.Index, '소통및업무협조'])) / 3)
        except Exception as e:
            logging.info(e)

    # could not convert string to float 오류 핸들링
    df.dropna(subset=['점수'], inplace=True)
    
    df['p'] = np.nan
    for row in df.itertuples():
        try:
            if df.loc[row.Index, '점수'] >= 9:
                df.loc[row.Index, 'p'] = 1
            else:
                df.loc[row.Index, 'p'] = 0
        except Exception as e:
            logging.info(e)
            pass

    df['d'] = np.nan
    for row in df.itertuples():
        try:
            if df.loc[row.Index, '점수'] <= 6:
                df.loc[row.Index, 'd'] = 1
            else:
                df.loc[row.Index, 'd'] = 0
        except Exception as e:
            logging.info(e)
            pass

    df['반기'] = ''
    for row in df.itertuples():
        try:
            if df.loc[row.Index, '평가시기'].split('.')[1] in ('1Q', '2Q'):
                df.loc[row.Index, '반기'] = '상반기'
            else:
                df.loc[row.Index, '반기'] = '하반기'
        except Exception as e:
            logging.info(e)
            pass
    
    df['진료과'] = df['피평가부서_RAW'].str.split('_').str[2]
    
    df['진료부구분'] = ''
    for row in df.itertuples():
        try:
            # 부천 
            if df.loc[row.Index, '피평가부서_RAW'].split('_')[0] == '부천':
                if df.loc[row.Index, '피평가부서_RAW'].split('_')[1] == '진료부':
                    
                    # 내과부
                    if df.loc[row.Index, '피평가부서_RAW'].split('_')[2] \
                        in ('호흡기내과', '알레르기내과', '신장내과',
                            '소화기내과', '내분비내과', '감염내과',
                            '가정의학과'):
                        df.loc[row.Index, '진료부구분'] = '내과부'

                    # 소아청소년부
                    elif df.loc[row.Index, '피평가부서_RAW'].split('_')[2] == '소아청소년과':
                        df.loc[row.Index, '진료부구분'] = '소아청소년부'

                    # 신경재활의학부
                    elif df.loc[row.Index, '피평가부서_RAW'].split('_')[2] \
                            in ('정신건강의학과', '재활의학과', '신경외과', '신경과'):
                        df.loc[row.Index, '진료부구분'] = '신경재활의학부'

                    # 심장내과부
                    elif df.loc[row.Index, '피평가부서_RAW'].split('_')[2] == '심장내과':
                        df.loc[row.Index, '진료부구분'] = '심장내과부'

                    # 외과부
                    elif df.loc[row.Index, '피평가부서_RAW'].split('_')[2] \
                            in ('피부과', '치과',
                                '정형외과', '이비인후과',
                                '외과', '산부인과',
                                '비뇨의학과'):
                        df.loc[row.Index, '진료부구분'] = '외과부'

                    # 임상지원부
                    elif df.loc[row.Index, '피평가부서_RAW'].split('_')[2] \
                            in ('진단검사의학과', '영상의학과',
                                '병리과', '마취통증의학과'):
                        df.loc[row.Index, '진료부구분'] = '임상지원부'
                        
                    # 중환자응급의학부
                    elif df.loc[row.Index, '피평가부서_RAW'].split('_')[2] \
                            in ('중환자의학과', '응급의학과'):
                        df.loc[row.Index, '진료부구분'] = '중환자응급의학부'
                        
                    # 흉부외과
                    elif df.loc[row.Index, '피평가부서_RAW'].split('_')[2] \
                            in ('소아흉부외과', '성인흉부외과', '흉부외과'):
                        df.loc[row.Index, '진료부구분'] = '흉부외과부'

                    # 과거자료 처리용
                   
                    # 감염병센터
                    elif df.loc[row.Index, '피평가부서_RAW'].split('_')[2] == '감염병센터':
                        df.loc[row.Index, '진료부구분'] = '감염병센터'

                    # 뇌혈관센터(19년 이전 자료)
                    elif df.loc[row.Index, '피평가부서_RAW'].split('_')[2] == '뇌혈관센터':
                        df.loc[row.Index, '진료부구분'] = '뇌혈관센터'

                    # 서울여성센터(19년 이전 자료)
                    elif df.loc[row.Index, '피평가부서_RAW'].split('_')[2] == '서울여성센터':
                        df.loc[row.Index, '진료부구분'] = '서울여성센터'

                    # 한길안센터(19년 이전 자료)
                    elif df.loc[row.Index, '피평가부서_RAW'].split('_')[2] == '한길안센터':
                        df.loc[row.Index, '진료부구분'] = '한길안센터'

                # 진료부 외의 값 처리
                else:
                    df.loc[row.Index, '진료부구분'] = df.loc[row.Index, '피평가부서_RAW'].split('_')[1]

            # 인천
            else: 
                if df.loc[row.Index, '피평가부서_RAW'].split('_')[1] == '진료부':
 
                    # 내과부
                    if df.loc[row.Index, '피평가부서_RAW'].split('_')[2] \
                            in ('호흡기내과', '알레르기내과',
                                '신장내과', '소화기내과',
                                '내분비내과', '감염내과',
                                '류마티스내과'):
                        df.loc[row.Index, '진료부구분'] = '내과부'

                    # 소아청소년부
                    elif df.loc[row.Index, '피평가부서_RAW'].split('_')[2] == '소아청소년과':
                        df.loc[row.Index, '진료부구분'] = '소아청소년부'

                    # 신경재활의학부
                    elif df.loc[row.Index, '피평가부서_RAW'].split('_')[2] \
                            in ('정신건강의학과', '재활의학과',
                                '신경외과', '신경과'):
                        df.loc[row.Index, '진료부구분'] = '신경재활의학부'
                        
                    # 심장내과부
                    elif df.loc[row.Index, '피평가부서_RAW'].split('_')[2] == '심장내과':
                        df.loc[row.Index, '진료부구분'] = '심장내과부'

                    # 외과부
                    elif df.loc[row.Index, '피평가부서_RAW'].split('_')[2] \
                            in ('피부과', '치과',
                                '정형외과', '이비인후과',
                                '외과', '비뇨의학과'):
                        df.loc[row.Index, '진료부구분'] = '외과부'

                    # 임상지원부
                    elif df.loc[row.Index, '피평가부서_RAW'].split('_')[2] \
                            in ('진단검사의학과', '영상의학과',
                                '병리과', '마취통증의학과',
                                '가정의학과'):
                        df.loc[row.Index, '진료부구분'] = '임상지원부'

                    # 중환자응급의학부
                    elif df.loc[row.Index, '피평가부서_RAW'].split('_')[2] in ('중환자의학과', '응급의학과'):
                        df.loc[row.Index, '진료부구분'] = '중환자응급의학부'

                    # 흉부외과
                    elif df.loc[row.Index, '피평가부서_RAW'].split('_')[2] == '흉부외과':
                        df.loc[row.Index, '진료부구분'] = '흉부외과부'

                    # 감염병센터
                    elif df.loc[row.Index, '피평가부서_RAW'].split('_')[2] == '감염병센터':
                        df.loc[row.Index, '진료부구분'] = '감염병센터'

                    # 뇌혈관센터
                    elif df.loc[row.Index, '피평가부서_RAW'].split('_')[2] == '뇌혈관센터':
                        df.loc[row.Index, '진료부구분'] = '뇌혈관센터'

                    # 서울여성센터
                    elif df.loc[row.Index, '피평가부서_RAW'].split('_')[2] == '서울여성센터':
                        df.loc[row.Index, '진료부구분'] = '서울여성센터'

                    # 한길안센터
                    elif df.loc[row.Index, '피평가부서_RAW'].split('_')[2] == '한길안센터':
                        df.loc[row.Index, '진료부구분'] = '한길안센터'

                    # 과거자료 처리용
                    elif df.loc[row.Index, '피평가부서_RAW'].split('_')[2] == '건강증진팀':
                        df.loc[row.Index, '진료부구분'] = '건강증진팀'
                
                # 진료부 외의 값 처리
                else:
                    df.loc[row.Index, '진료부구분'] = df.loc[row.Index, '피평가부서_RAW'].split('_')[1]
        except Exception as e:
            logging.info(e)
            pass

    return df


def collaboration_return_data():
    
    RANGE = '!A2:F'  # 범위 지정 

    # googlesheet API를 통해 시트list를 불러온다
    sheet, SPREADSHEET_ID = call_google_sheet()
    values = []

    logging.info(" ------------------------  콜라보 자료 취합 시작 ------------------------")
    try:
        cnt = 0
        for i in SPREADSHEET_ID:
            sheet_metadata = sheet.get(spreadsheetId=i).execute()
            sheets = sheet_metadata.get('sheets', '')
            sheet_name = sheets[0].get('properties', {}).get('title', '')
            
            result = sheet.values().get(spreadsheetId=i,
                                        range=sheet_name + RANGE).execute()

            values.append(result.get('values', [[]]))

            cnt += 1
            logging.info(sheet_metadata.get('properties', {}).get('title', '') + " 전환 완료..!")

            # HttpError 429 발생시 sleep을 더 자주 발생시켜야함
            if cnt % 5 == 0:
                logging.info("row: " + str(cnt))
                logging.info("Sleeping...Process will continue in 100 seconds")
                time.sleep(100)
    except Exception as e:
        logging.info(f"오류 종류: {type(e).__name__}, {e}")

    df = pd.DataFrame(row for value in values for row in value)
    try:
        df.columns = ['평가부서_RAW', '피평가부서_RAW', '친절', '신뢰', '소통및업무협조', '의견']
    except ValueError:
        df['의견'] = ''
    finally:
        df.columns = ['평가부서_RAW', '피평가부서_RAW', '친절', '신뢰', '소통및업무협조', '의견']

    df_etl = df_column_etl(df)
    df_etl = df_etl[['평가시기', '평가기준일', '평가부서기구',
                     '평가부서_RAW', '피평가부서_RAW',
                     '친절', '신뢰', '소통및업무협조',
                     '의견', '점수',
                     'p', 'd',
                     '반기', '진료부구분',
                     '진료과']]

    # airflow 에서는 삭제
    # writer = pd.ExcelWriter('/opt/airflow/data/Collaboration_rawdata.xlsx', engine='xlsxwriter')
    # df_etl.to_excel(writer, index=False, sheet_name='rawdata')
    # writer.save()

    return df_etl



