import cx_Oracle
import logging
from datetime import datetime, timedelta
# DAG object
from airflow import DAG
# Operators
from airflow.operators.python import PythonOperator
# airflow에 저장된 변수 불러오기
from airflow.models import Variable


def print_sql_results():
    """adw에서부터 정보를 받고 출력한다."""
    TNS_ADMIN = Variable.get("TNS_ADMIN")
    cx_Oracle.init_oracle_client(config_dir=TNS_ADMIN)
    connection = cx_Oracle.connect(
        user=Variable.get("ADW_USER"),
        password=Variable.get("ADW_PASSWORD"),
        dsn=Variable.get("ADW_SID"))
    c = connection.cursor()
    result = c.execute('SELECT * FROM DW.재원환자리스트 where rownum < 10')
    for row in result:
        logging.info(row)


# [START default_args]
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
}


with DAG(
    'pull_sql_example_ver1',
    default_args=default_args,
    description='ADW에서 자료를 가져오는 dag예제입니다.',

    start_date=datetime(2021, 9, 28),
    # schedule_interval= '@daily'
    schedule_interval=None
) as dag:
    print_log = PythonOperator(task_id="print_query",
                               python_callable=print_sql_results,
                               )
    print_log
