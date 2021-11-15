import cx_Oracle
import logging
from datetime import timedelta
from airflow.models import Variable
# airflow module
from airflow import DAG
from airflow.utils.dates import days_ago
# Operators
from airflow.operators.python import PythonOperator
""" 모든 입력 파라미터는 AIRFLOW VARIABLE에서 가져오는 것으로 한다."""


class ADW_connection_cx_oracle(cx_Oracle.Connection):
    """ADW initialization."""

    def __init__(self) -> None:

        self._tns_admin: str = Variable.get("TNS_ADMIN")
        self._user: str = Variable.get("ADW_USER")
        self._password: str = Variable.get("ADW_PASSWORD")
        self._dsn: dict = Variable.get("ADW_SID")

        # cx_oralce version check
        logging.info(f'cx_Oracle Version: {cx_Oracle.__version__}')

        try:
            cx_Oracle.init_oracle_client(config_dir=self._tns_admin)
        except Exception as e:
            logging.info(f"원인: {e}")

        super(ADW_connection_cx_oracle, self)\
            .__init__(self._user, self._password, self._dsn)

    def cursor(self):
        return MyCursor(self)


class MyCursor(cx_Oracle.Cursor):

    def execute(self, query, args: dict = ()):

        logging.info(f"Executing:, {query}")
        try:
            return super(MyCursor, self).execute(query, args)

        except cx_Oracle.DatabaseError as e:
            logging.info("실행이 실패하였습니다.")
            logging.info(f"원인: {e}")

    def executemany(self, query, args):

        logging.info(f"Executing:, {query}")
        try:
            return super(MyCursor, self).executemany(query, args)
        except cx_Oracle.DatabaseError as e:
            logging.info("실행이 실패하였습니다.")
            logging.info(f"원인: {e}")


def test_database():
    with ADW_connection_cx_oracle() as connection:
        logging.info(f'Oracle DB Version: {connection.version}')


def test_cursor():
    with ADW_connection_cx_oracle() as connection:
        with connection.cursor() as cur:
            result = cur.execute("""select 'test complete' from dual""")
            results = result.fetchone()
            logging.info(results)


# Dag
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}


with DAG('database_connection_test',
         description="""데이터베이스 연결 테스트 용 dag입니다.""",
         start_date=days_ago(1, 0, 0, 0, 0),
         max_active_runs=1,
         schedule_interval=None,
         default_args=default_args,
         catchup=False
         ) as dag:

    test_log_task = PythonOperator(
        task_id="test_log_task",
        python_callable=test_database,
    )
    test_cursor_task = PythonOperator(
        task_id="test_cursor_task",
        python_callable=test_cursor,
    )

    # task flow
    test_log_task >> test_cursor_task
