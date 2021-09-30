import cx_Oracle
import logging
from airflow.models import Variable


def call_sql_files(file_path, file_name):
    """ adw에서부터 정보를 받고 출력한다.
        단순 select문은 cursor().execute()
        select문 이외는 cursor().executescript() -- semi 콜론인식가능으로 여러 명령 실행 가능
        으로 실행한다.
    """
    sql_file = f'{file_path}{file_name}.sql'

    TNS_ADMIN = Variable.get("TNS_ADMIN")
    try:
        cx_Oracle.init_oracle_client(config_dir=TNS_ADMIN)
        connection = cx_Oracle.connect(
            user=Variable.get("ADW_USER"),
            password=Variable.get("ADW_PASSWORD"),
            dsn=Variable.get("ADW_SID"))
        c = connection.cursor()
        logging.info("연결이 성공하였습니다.")
        sql_file = open(sql_file).read()
        data = c.execute(sql_file).fetchall()
        c.close()
        return data
    except Exception as e:  
        logging.info("연결이 실패하였습니다.")
        logging.info(f"원인: {e}")
        c.close()
