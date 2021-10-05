import cx_Oracle
import logging
from datetime import datetime, timedelta
# DAG object
from airflow import DAG
# Operators
from airflow.operators.python import PythonOperator
# airflow에 저장된 변수 불러오기
from airflow.models import Variable


sql = """MERGE INTO  dw.임시테이블 a
USING 
        (
            SELECT 
                      a.병원명
                    , a.수익마감일자
                    , a.입원외래검진구분
                    , SUM(a.수익_총수익) 수익_총수익
            
            FROM DW."수익_처방별_전체" a
            WHERE a.수익마감일자 between to_date(:1,'YYYYMMDD') and to_date(:2,'YYYYMMDD') 
            GROUP BY  a.병원명
                    , a.수익마감일자
                    , a.입원외래검진구분
        ) b
ON
        (
                  a.병원명 = b.병원명
             AND  a.수익마감일자 = b.수익마감일자
             AND  a.입원외래검진구분 = b.입원외래검진구분
         )
WHEN MATCHED THEN 
        UPDATE SET a.수익_총수익 = b.수익_총수익       

WHEN NOT MATCHED THEN 
        INSERT ( 
                    병원명
                  , 수익마감일자
                  , 입원외래검진구분
                  , 수익_총수익
                )  
        VALUES (
                    b.병원명
                  , b.수익마감일자
                  , b.입원외래검진구분
                  , b.수익_총수익
                )  """


def print_sql_results():
    """adw에서부터 정보를 받고 출력한다."""
    TNS_ADMIN = Variable.get("TNS_ADMIN")
    cx_Oracle.init_oracle_client(config_dir=TNS_ADMIN)
    connection = cx_Oracle.connect(
        user=Variable.get("ADW_USER"),
        password=Variable.get("ADW_PASSWORD"),
        dsn=Variable.get("ADW_SID"))
    c = connection.cursor()
    date_parameter = {
                    "1": str(Variable.get("START_DATE")),  # 시작일자
                    "2": str(Variable.get("END_DATE"))  # 종료일자
                     }


    # result = c.execute('SELECT * FROM DW.재원환자리스트 where rownum < 10')
    c.execute(sql, date_parameter)
    connection.commit()
    # for row in result:
    #     logging.info(row)


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


# with DAG(
#     'pull_sql_example_ver1',
#     default_args=default_args,
#     description='ADW에서 자료를 가져오는 dag예제입니다.',

#     start_date=datetime(2021, 10, 5),
#     # schedule_interval= '@daily'
#     schedule_interval=None
# ) as dag:
#     print_log = PythonOperator(task_id="print_query",
#                                python_callable=print_sql_results,
#                                )
#     print_log
