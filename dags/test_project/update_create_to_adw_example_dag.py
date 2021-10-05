import logging
from datetime import datetime, timedelta
# DAG object
from airflow import DAG
from airflow.models import Variable

# Operators
from airflow.operators.python import PythonOperator
# airflow에 저장된 변수 불러오기
# from airflow.models import Variable
from scripts.call_data import ADW_connection


# ADW Connection setup
con = ADW_connection()
cur = con.cursor()


# 업데이트, MERGE, INSERT작업용
def update_sql_results(sql_path, sql_name):

    logging.info("데이터를 호출합니다. from create_sql_results()")

    try:
        query = con.get_query_from_file(sql_path, sql_name)

        # 오라클에서는 bind 변수를 숫자로 넘겨줘야함
        # bind 변수 정의
        args = {
            "1": str(Variable.get("START_DATE")),  # 시작일자
            "2": str(Variable.get("END_DATE"))  # 종료일자
              }

        cur.execute(query, args)    
        con.commit()
        # cur.close()
        # con.close()
        
        logging.info("작업 완료")

    except Exception as e:
        logging.info(f'<<오류 발생>> -> {e}')


# SELECT 작업용
def select_sql_results(sql_path, sql_name):
    logging.info("데이터를 호출합니다. select_sql_results()")
    try:
        query = con.get_query_from_file(sql_path, sql_name)

        # 오라클에서는 bind 변수를 숫자로 넘겨줘야함
        # bind 변수 정의
        args = {
            "1": str(Variable.get("START_DATE")),  # 시작일자
            "2": str(Variable.get("END_DATE"))  # 종료일자
              }

        data = cur.execute(query, args).fetchall()
        # cur.close()
        # con.close()

        logging.info(data[:4])

    except Exception as e:
        logging.info(f'<<오류 발생>> -> {e}')


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG('query_example_1',
         description='ADW에 테이블 조작하는 DAG입니다.',
         start_date=datetime(2021, 10, 5),
         max_active_runs=1,
         schedule_interval=None,
         default_args=default_args,
         catchup=False
         ) as dag:

    merge_task = PythonOperator(task_id="update_query",
                                python_callable=update_sql_results,
                                op_kwargs={
                                    'sql_path': '/opt/airflow/dags/test_project/sqls/',
                                    'sql_name': '수익_처방별_UPDATE.sql'
                                           }
                                )
    select_task = PythonOperator(task_id="select_query",
                                 python_callable=select_sql_results,
                                 op_kwargs={
                                    'sql_path': '/opt/airflow/dags/test_project/sqls/',
                                    'sql_name': 'M_진료협력.sql'
                                           }
                                 )
    merge_task >> select_task
