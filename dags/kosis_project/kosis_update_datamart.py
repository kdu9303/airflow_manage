# -*- coding: utf-8 -*-
import os
import time
import logging
from airflow.exceptions import AirflowException
# database module
from scripts.call_database import ADW_connection_cx_oracle


def update_total_census_population(conn: ADW_connection_cx_oracle,
                                   sql_path: str,
                                   sql_name: str):
    """총인구조사 테이블에 CENSUS_POLULATION 테이블 데이터를 업데이트한다"""
    try:
        with conn.cursor() as cur:

            startTime = time.time()
            query = conn.get_query_from_file(sql_path, sql_name)

            cur.execute(query)
            conn.commit()

            logging.info("DataMart 업데이트를 시작합니다")

            endTime = time.time()

        conn.close()
        logging.info("작업 완료")
        logging.info(f'실행 시간: {round(endTime-startTime,2)}초')

    except Exception as e:
        raise AirflowException(
            f"{update_total_census_population.__name__} --> {e}"
            )


# main
def update_total_census_population_main():

    # Database initialization
    conn = ADW_connection_cx_oracle()

    sql_path = '/opt/airflow/dags/kosis_project/sql'
    sql_files = os.listdir(sql_path)

    # Merge, insert파일 실행
    for file in sql_files:
        logging.info(f'{file.split(".")[0]} 업데이트 중...')
        update_total_census_population(conn, sql_path, file)


if __name__ == '__main__':
    update_total_census_population_main()
