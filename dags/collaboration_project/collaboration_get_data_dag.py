import logging
import time
from datetime import datetime, timedelta
from pytz import timezone
import pandas as pd
# airflow module
from airflow import DAG
from airflow.utils.dates import days_ago
# Operators
from airflow.operators.python import PythonOperator
# collaboration module
from collaboration_project.collaboration_get_data import collaboration_return_data
# database module
from scripts.call_database import ADW_connection_cx_oracle

# timezione setting
KST = timezone('Asia/Seoul')


# googlesheet to csv
def save_collaboration_data(table_name: str):
    logging.info("함수를 호출합니다. save_collaboration_data()")

    try:
        startTime = time.time()

        df = collaboration_return_data()
        # path = "/opt/airflow/data/collaboration_raw2.csv"
        # df.to_csv(path, index=False, header=False)
        with ADW_connection_cx_oracle() as con:
            with con.cursor() as cur:

                rows = [tuple(x) for x in df.values]
                
                cur.fast_executemany = True
                # 데이터 타입 확인 필수

                cur.executemany(
                                f"""MERGE INTO {table_name} a
                                    USING DUAL
                                    ON
                                    (    a.평가시기 = :1
                                     AND a.평가기준일 = :2
                                     AND a.평가부서기구 = :3
                                     AND a.평가부서_RAW = :4
                                     AND a.피평가부서_RAW = :5
                                     AND a.친절 = :6
                                     AND a.신뢰 = :7
                                     AND a.소통및업무협조 = :8
                                     AND NVL(a.의견,'') = NVL(:9,'')
                                     AND a.점수 = :10
                                     AND a.P = :11
                                     AND a.D = :12
                                     AND a.반기 = :13
                                     AND a.진료부구분 = :14
                                     AND a.진료과 = :15
                                    )
                                    WHEN NOT MATCHED THEN
                                    INSERT
                                    (  a.평가시기
                                     , a.평가기준일
                                     , a.평가부서기구
                                     , a.평가부서_RAW
                                     , a.피평가부서_RAW
                                     , a.친절
                                     , a.신뢰
                                     , a.소통및업무협조
                                     , a.의견
                                     , a.점수
                                     , a.P
                                     , a.D
                                     , a.반기
                                     , a.진료부구분
                                     , a.진료과
                                    )
                                    VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15)
                                """, rows)
                con.commit()

        endTime = time.time()

        logging.info("EXTRACT 성공")
        logging.info(f'실행 시간: {round(endTime-startTime, 2)}초')
    except Exception as e:
        logging.info(f'<<오류 발생>> -> {e}')


# Dag
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

with DAG('collaboration_data_example_1',
         description='Collaboration test DAG입니다.',
         start_date=days_ago(1, 0, 0, 0, 0),
         max_active_runs=1,
         schedule_interval=None,
         default_args=default_args,
         catchup=False
         ) as dag:

    get_collaboration_data_task = PythonOperator(task_id="get_collaboration_data_task",
                                                 python_callable=save_collaboration_data,
                                                 op_kwargs={
                                                            'table_name': 'DW.임시테이블'
                                                           }                                                 
                                                )
    get_collaboration_data_task
