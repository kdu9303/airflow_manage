import logging
from datetime import datetime, timedelta
# DAG object
from airflow import DAG
# Operators
from airflow.operators.python import PythonOperator
# airflow에 저장된 변수 불러오기
# from airflow.models import Variable
from scripts import call_data


def print_sql_results(file_path, file_name):
    data = call_data.call_sql_files(file_path, file_name)
    logging.info("데이터를 호출합니다.")
    logging.info(data[:4])
    # for row in data:
    #     logging.info(row)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG('parameterized_query',
         description='ADW에서 자료를 sql파일로부터 가져오는 dag예제입니다.',
         start_date=datetime(2021, 9, 1),
         max_active_runs=3,
         schedule_interval=None,
         default_args=default_args,
        # template_searchpath='/opt/airflow/dags/test_project/sqls',
         catchup=False
         ) as dag:

    print_log = PythonOperator(task_id="print_query",
                               python_callable=print_sql_results,
                               op_kwargs={
                                   'file_path': '/opt/airflow/dags/test_project/sqls/',
                                   'file_name': 'M_진료협력'
                                   
                                   }
                               )
    print_log
