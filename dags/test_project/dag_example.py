try:
    from datetime import datetime, timedelta
    from textwrap import dedent

# DAG object
    from airflow import DAG

# Operators 
    from airflow.operators.bash import BashOperator 
    from airflow.operators.python import PythonOperator
    from airflow.utils.dates import days_ago    
    print("모든 패키지 로드 완료")
except Exception as e:
    print(f"Error {e}")

# [START default_args]
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['kdu9303@sejongh.co.kr'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
}

# DAG config
with DAG(
    'first_dag_example_ver2',
    default_args = default_args,
    description = '첫번째 dag예제입니다, BASH명령어를 수행합니다.',

    start_date=datetime(2021,8,23),
    # schedule_interval= '@daily'
    schedule_interval=timedelta(minutes=1)
) as dag:
    task1 = BashOperator(
        task_id = 'bash_task1',
        bash_command = "echo 첫번째 task를 실행했습니다!"
    )
    task2 = BashOperator(
        task_id = 'bash_task2' ,
        bash_command = "echo 두번째 task를 실행했습니다!, 첫번째 task이후에 실행됩니다!"
    )
    task1 >> task2