# -*- coding: utf-8 -*-
import time
import logging
from airflow.exceptions import AirflowException
from pandas.core.frame import DataFrame
# kosis module
from kosis_project.kosis_get_total_census_data import return_cencus_data
# database module
from scripts.call_database import ADW_connection_cx_oracle


def upload_to_census_population(conn: ADW_connection_cx_oracle,
                                kosis_data: DataFrame,
                                table_name: str):
    """CENSUS POPULATION 테이블에 통계청 총인구수 데이터를 업로드한다"""
    try:
        with conn.cursor() as cur:

            startTime = time.time()

            rows = [tuple(x) for x in kosis_data.values]
            cur.fast_executemany = True

            logging.info("database 업로드를 시작합니다")
            cur.executemany(
                        f"""MERGE INTO {table_name} a
                            USING DUAL
                            ON
                            (
                                a.PRD_DE = :1
                            AND a.C1 = :2
                            AND a.C1_NM = :3
                            AND a.TBL_ID = :4
                            )
                            WHEN MATCHED THEN
                                UPDATE SET
                                    a.DT = :5
                            WHEN NOT MATCHED THEN
                            INSERT
                            (
                                a.PRD_DE
                                , a.C1
                                , a.C1_NM
                                , a.TBL_ID
                                , a.DT
                            )
                            VALUES (:1, :2, :3, :4, :5)
                        """, rows)
            conn.commit()

            endTime = time.time()

        conn.close()
        logging.info("작업 완료")
        logging.info(f'실행 시간: {round(endTime-startTime,2)}초')

    except Exception as e:
        raise AirflowException(
            f"{upload_to_census_population.__name__} --> {e}"
            )


# main
def census_population_main():

    # Database initialization
    conn = ADW_connection_cx_oracle()

    census_data = return_cencus_data()

    upload_to_census_population(conn, census_data, "dw.census_population")


if __name__ == '__main__':
    census_population_main()
