# from dataclasses import dataclass
import cx_Oracle
import logging
from airflow.models import Variable

""" 모든 입력 파라미터는 AIRFLOW VARIABLE에서 가져오는 것으로 한다."""


class ADW_connection(cx_Oracle.Connection):
    """ADW initialization."""

    def __init__(self, *args) -> None:

        self._tns_admin: str = Variable.get("TNS_ADMIN")
        self._user: str = Variable.get("ADW_USER")
        self._password: str = Variable.get("ADW_PASSWORD")
        self._dsn: dict = Variable.get("ADW_SID")

        # cx_oralce version check
        logging.info(f'cx_Oracle Version: {cx_Oracle.__version__}')

        cx_Oracle.init_oracle_client(config_dir=self._tns_admin)

        super(ADW_connection, self).__init__(self._user, self._password, self._dsn)

    def cursor(self):
        return MyCursor(self)

    def get_query_from_file(self, sql_path, sql_name):
        """query를 파일로부터 불러온다."""

        try:
            sql_file = f'{sql_path}{sql_name}'
            query = open(sql_file).read()
            return query
        except IOError as e:
            logging.info("파일을 불러오는데 실패했습니다")    
            logging.info(e)


class MyCursor(cx_Oracle.Cursor):

    def execute(self, query, args):

        logging.info(f"Executing:, {query}")
        try:
            return super(MyCursor, self).execute(query, args)
        except cx_Oracle.DatabaseError as e:
            logging.info("실행이 실패하였습니다.")
            logging.info(f"원인: {e}")






# class Database:
#     """adw와 연결하는 하는 클래스."""

#     def __init__(self):
#         self._tns_admin: str = Variable.get("TNS_ADMIN")
#         self._user: str = Variable.get("ADW_USER")
#         self._password: str = Variable.get("ADW_PASSWORD")
#         self._dsn: dict = Variable.get("ADW_SID")

#         # self.uri = f'oracle+cx_oracle://{self._user}:{self._password}@{self._dsn}' 

#     # to implement objects in with statement
#     # def __enter__(self):

#     #     return self

#     # def __exit__(self, exc_type, exc_val, exc_tb):
#     #     self.close()

#     def connection(self):

#         logging.info(f'cx_Oracle Version: {cx_Oracle.__version__}')

#         cx_Oracle.init_oracle_client(config_dir=self._tns_admin)
#         try:
#             connection = cx_Oracle.connect(
#                 user=self._user,
#                 password=self._password,
#                 dsn=self._dsn)
#             # connection.execution_options(autocommit=True)    

#             # pool = cx_Oracle.SessionPool(
#             #     user=self._user,
#             #     password=self._password,
#             #     dsn=self._dsn,
#             #     min=2,
#             #     max=5,
#             #     increment=1
#             # )
#             # connection = pool.acquire()

#             return connection

#         except cx_Oracle.DatabaseError as e:
#             logging.info("연결이 실패하였습니다.")
#             logging.info(f"원인: {e}")

#     def cursor(self):
#         return self.connection().cursor()            

#     def get_query(self, sql_path, sql_name):
#         """ query를 파일로부터 불러온다."""
#         try:
#             sql_file = f'{sql_path}{sql_name}'
#             query = open(sql_file).read()
#             return query
#         except IOError as e:
#             logging.info("파일을 불러오는데 실패했습니다")    
#             logging.info(e)

#     def select(self, sql_path, sql_name) -> tuple:
#         """주의: 이중 tuple로 리턴한다."""
#         try:
#             query = self.get_query(sql_path, sql_name)

#             # 오라클에서는 bind 변수를 숫자로 넘겨줘야함
#             date_parameter = {
#                               "1": str(Variable.get("START_DATE")),  # 시작일자
#                               "2": str(Variable.get("END_DATE"))  # 종료일자
#                             }

#             data = self.cursor().execute(query, date_parameter).fetchall()
#             return data
#         except Exception as e:
#             logging.info(f'오류 원인: {e}')

#     def update(self, sql_path, sql_name) -> None:
#         """
#         create, update, insert, merge문을 실행한다.
#         """

#         query = self.get_query(sql_path, sql_name)
#         try:

#             date_parameter = {
#                               "1": str(Variable.get("START_DATE")),  # 시작일자
#                               "2": str(Variable.get("END_DATE"))  # 종료일자
#                             }

#             self.cursor().prefetchrows = 1000
#             self.cursor().arraysize = 1000

#             self.cursor().execute(query, date_parameter)
#             self.connection().commit()
#             logging.info(f"{sql_name}파일에 대한 변경 및 생성에 성공하였습니다.")
#         except cx_Oracle.DatabaseError as e:
#             self.connection().rollback()
#             logging.info(f'오류 원인: {e}')

