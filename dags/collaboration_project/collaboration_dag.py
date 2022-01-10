import pendulum
from datetime import datetime, timedelta
# from pytz import timezone
# airflow module
from airflow import DAG
# from airflow.utils.dates import days_ago
# Operators
from airflow.operators.python import PythonOperator
# collaboration module
from collaboration_project.collaboration_upload_data import (
    upload_to_collaboration
)


KST = pendulum.timezone("Asia/Seoul")


# Dag
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG('collaboration_collecting_data',
         description="""Collaboration 자료를 dw로 전송하는 DAG입니다.
                        평가시기와 평가기준일이 현재월 기준이기때문에
                        평가 월 이외 에는 실행하면 날짜가 틀어집니다.""",
         start_date=datetime(2022, 1, 1, tzinfo=KST),
         max_active_runs=1,
         schedule_interval=None,
         default_args=default_args,
         catchup=False
         ) as dag:

    run_collaboration_data_task = PythonOperator(
        task_id="run_collaboration_data_task",
        python_callable=upload_to_collaboration,
        op_kwargs={
            'table_name': 'DW.collaboration'
        }
    )
    run_collaboration_data_task
