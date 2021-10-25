import logging
import time
from datetime import timedelta
# airflow module
from airflow import DAG
from airflow.utils.dates import days_ago
# Operators
from airflow.operators.python import PythonOperator
# database module
from scripts.call_database import ADW_connection_cx_oracle
# youtube module
from youtube_project.youtube_get_channel_info import return_channel_statistics  # 채널 정보
# from youtube_project.youtube_get_video_info import return_channel_videos  # 채널 비디오 정보


# youtube api to database
def save_yutube_channel_info(table_name: str):
    logging.info("함수를 호출합니다. save_yutube_channel_info()")

    try:
        startTime = time.time()

        df = return_channel_statistics()
        logging.info(df.columns)
        with ADW_connection_cx_oracle() as con:
            with con.cursor() as cur:

                rows = [tuple(x) for x in df.values]
                logging.info(rows)
                cur.fast_executemany = True

                # bind 데이터 타입 확인 필수(특히 date형식)
                cur.executemany(
                    f"""MERGE INTO {table_name} a
                        USING DUAL
                        ON
                        (
                            a.CHANNELID = :1
                        AND a.BASEDATE = :2
                        )
                        WHEN MATCHED THEN
                            UPDATE SET
                                  a.VIEWCOUNT = :3
                                , a.SUBSCRIBERCOUNT = :4
                                , a.VIDEOCOUNT = :5
                        WHEN NOT MATCHED THEN
                        INSERT
                        (
                              a.CHANNELID
                            , a.BASEDATE
                            , a.VIEWCOUNT
                            , a.SUBSCRIBERCOUNT
                            , a.VIDEOCOUNT
                        )
                        VALUES (:1, :2, :3, :4, :5)
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
    'retries': 3,
    'retry_delay': timedelta(minutes=3)
}


with DAG('youtube_collecting_data',
         description="""Youtube 정보를 가져옵니다.""",
         start_date=days_ago(1, 0, 0, 0, 0),
         max_active_runs=1,
         schedule_interval="0 */6 * * *",
         default_args=default_args,
         catchup=False
         ) as dag:

    get_channel_info_task = PythonOperator(
        task_id="get_channel_info_task",
        python_callable=save_yutube_channel_info,
        op_kwargs={
            'table_name': 'DW.youtube_channel_info'
        }
    )
    get_channel_info_task
