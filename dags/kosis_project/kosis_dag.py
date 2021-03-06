import pendulum
from datetime import datetime, timedelta
# airflow module
from airflow import DAG
# from airflow.utils.dates import days_ago
# Operators
from airflow.operators.python import PythonOperator
# kosis module
from kosis_project.kosis_upload_data import census_population_main
from kosis_project.kosis_update_datamart import update_census_datamart_main

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


with DAG('kosis_collecting_data',
         description="""KOSIS 정보를 가져옵니다.""",
         start_date=datetime(2022, 1, 1, tzinfo=KST),
         max_active_runs=1,
         schedule_interval="0 9 * * 2,5",
         default_args=default_args,
         catchup=False
         ) as dag:

    # 1단계
    # kosis api에서 CENSUS_POPULATION 테이블로 업로드
    run_census_population_task = PythonOperator(
        task_id="run_census_population_task",
        python_callable=census_population_main
        # op_kwargs={
        #     'table_name': 'DW.census_population'
        # }
    )

    # 2단계
    # CENSUS_POPULATION 테이블에서 파생되는 DATA MART를 업데이트
    run_update_census_datamart_main_task = PythonOperator(
        task_id="run_update_census_datamart_main_task",
        python_callable=update_census_datamart_main
    )

    # task flow
    run_census_population_task >> run_update_census_datamart_main_task
