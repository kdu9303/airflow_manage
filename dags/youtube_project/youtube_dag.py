from pandas.core.frame import DataFrame
import pendulum
from datetime import datetime, timedelta
# airflow module
from airflow import DAG
# from airflow.utils.dates import days_ago
# Operators
from airflow.operators.python import PythonOperator
# youtube module
from youtube_project.youtube_upload_data import upload_yutube_channel_info
from youtube_project.youtube_upload_data import upload_yutube_video_info
from youtube_project.youtube_upload_data import upload_yutube_video_stats


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


with DAG('youtube_collecting_data',
         description="""Youtube 정보를 가져옵니다.""",
         start_date=datetime(2022, 1, 1, tzinfo=KST),
         max_active_runs=1,
         schedule_interval="0 9,20 * * 1-5",
         default_args=default_args,
         catchup=False
         ) as dag:

    upload_yutube_channel_info_task = PythonOperator(
        task_id="upload_yutube_channel_info_task",
        python_callable=upload_yutube_channel_info,
        op_kwargs={
            'table_name': 'DW.youtube_channel_info'
        }
    )

    upload_yutube_video_info_task = PythonOperator(
        task_id="upload_yutube_video_info_task",
        python_callable=upload_yutube_video_info,
        op_kwargs={
            'table_name': 'DW.youtube_video_info'
        }
    )

    upload_yutube_video_stats_task = PythonOperator(
        task_id="upload_yutube_video_stats_task",
        python_callable=upload_yutube_video_stats,
        op_kwargs={
            'table_name': 'DW.youtube_video_stats'
        }
    )

    # task flow
    upload_yutube_channel_info_task >> upload_yutube_video_info_task >> upload_yutube_video_stats_task
