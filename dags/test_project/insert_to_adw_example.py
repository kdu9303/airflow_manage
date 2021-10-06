import logging
import time
from datetime import datetime, timedelta
from pytz import timezone
import modin.pandas as pd
# DAG object
from airflow import DAG
# from airflow.models import Variable
from airflow.utils.dates import days_ago

# Operators
from airflow.operators.python import PythonOperator
# airflow에 저장된 변수 불러오기
# from airflow.models import Variable
from scripts.call_data import ADW_connection_cx_oracle

#timezione
KST = timezone('Asia/Seoul')

# ADW Connection setup
con = ADW_connection_cx_oracle()


# SELECT 작업용
def select_sql_results(sql_path, sql_name):
    logging.info("데이터를 호출합니다. select_sql_results()")

    try:
        
        startTime = time.time()

        query = con.get_query_from_file(sql_path, sql_name)

        # 시간 정의
        current_date = datetime.now(KST)
        start_date = (current_date-timedelta(days=1)).strftime("%Y%m%d")
        end_date = (current_date-timedelta(days=1)).strftime("%Y%m%d")

        # 오라클에서는 bind 변수를 숫자로 넘겨줘야함
        args = {
            "1": start_date,  # 시작일자
            "2": end_date,  # 종료일자
            # "1": str(Variable.get("START_DATE")),  # 시작일자
            # "2": str(Variable.get("END_DATE"))  # 종료일자
            # "1": '20210109',  # 시작일자
            # "2": '20210109'  # 종료일자
            }

        # data = cur.execute(query, args).fetchall()
        # logging.info(data[:1])

        # dataframe으로 저장
        df = pd.read_sql(query, con, params=args)
        
        logging.info(df[:2])

        path = "/opt/airflow/data/수익_처방별_INSERT.csv"
        df.to_csv(path,
                  index=False,
                  header=False
                  )

        endTime = time.time()

        logging.info("EXTRACT 성공")
        logging.info(f'실행 시간: {round(endTime-startTime,2)}초')
    except Exception as e:
        logging.info(f'<<오류 발생>> -> {e}')


def insert_sql_results(sql_path, sql_name):
    """select문 결과를 메모리에 저장 후 insert 한다."""
    pass


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG('insert_query_example_1',
         description='ADW에 테이블 INSERT하는 DAG입니다.',
        #  start_date=datetime(2021, 10, 6),
         start_date=days_ago(0, 0, 0, 0, 0),
         max_active_runs=1,
         schedule_interval=None,
         default_args=default_args,
         catchup=False
         ) as dag:

    select_task = PythonOperator(task_id="select_query",
                                 python_callable=select_sql_results,
                                 op_kwargs={
                                    'sql_path': '/opt/airflow/dags/test_project/sqls/',
                                    'sql_name': '수익_처방별_INSERT.sql'
                                           }
                                 )
    # merge_task >> select_task
    select_task


