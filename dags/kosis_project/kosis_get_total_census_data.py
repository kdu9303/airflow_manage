# -*- coding: utf-8 -*-
import requests
import datetime
from dateutil.relativedelta import relativedelta
import json
import logging
import pandas as pd
from airflow.models import Variable

# 로그 기록용
logger = logging.getLogger()


def get_census_data(api_key: str) -> list:

    # 전월, 전전월자료를 가져온다
    cur_month = datetime.date.today().strftime('%Y%m')

    prev_month1 = (
        datetime.date.today() + relativedelta(months=-1)
        ).strftime('%Y%m')
    prev_month2 = (
        datetime.date.today() + relativedelta(months=-2)
        ).strftime('%Y%m')

    startPrdDe = [int(prev_month2), int(prev_month1), int(cur_month)]

    population = []

    for date in startPrdDe:
        url = (
            'https://kosis.kr/openapi/Param/statisticsParameterData.do?'
            f'method=getList&apiKey={api_key}&'
            'itmId=T20+&objL1=ALL&objL2=&objL3=&objL4=&'
            'objL5=&objL6=&objL7=&objL8=&'
            'format=json&jsonVD=Y&prdSe=M&'
            f'startPrdDe={date}&endPrdDe={date}&'
            'loadGubun=2&orgId=101&tblId=DT_1B040A3'
        )

        try:
            r = requests.get(url)

            if r.status_code == 200:

                # 데이터가 존재하지않으면 스킵한다
                if 'err' in eval(r.text):
                    continue

                population.append(json.loads(r.text))

            else:
                r.close()
        except Exception as e:
            logger.exception(f"{get_census_data.__name__} --> {e}")
            raise

    return population


def data_transform(population):

    try:
        df = pd.concat(
            pd.DataFrame([v]) for row in population for j, v in enumerate(row)
            )

    #    df = pd.concat(pd.DataFrame([v]) for i, v in enumerate(population))
        df['DT'] = df['DT'].astype('int')

        # 년월 + 일자를 붙인다
        df['PRD_DE'] = pd.to_datetime(df['PRD_DE'] + '01')

        col = ['PRD_DE', 'C1', 'C1_NM', 'TBL_ID', 'DT']
        df = df[col].reset_index(drop=True)

    except ValueError:
        logger.warning(f"{data_transform.__name__} \
                         --> No data returned from api request")

    except UnboundLocalError:
        logger.warning(f"{data_transform.__name__} \
                         --> No data returned from api request")

    except Exception as e:
        logger.exception(f"{data_transform.__name__} --> {e}")
    return df


# main
def return_cencus_data():

    # KOSIS API
    api_key = Variable.get('KOSIS_API_KEY')
    # api_key = ''  # 검증용

    population = get_census_data(api_key)

    transformed_data = data_transform(population)

    return transformed_data


if __name__ == '__main__':
    print(return_cencus_data())
