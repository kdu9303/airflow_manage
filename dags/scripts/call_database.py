import cx_Oracle
import logging
from airflow.models import Variable
# Dataframe 업로드용
# from sqlalchemy import types, create_engine
# from pandas.io.sql import to_sql, read_sql

# 로그 기록용
logger = logging.getLogger()


class ADW_connection_cx_oracle(cx_Oracle.Connection):
    """ADW initialization."""

    def __init__(self, *args) -> None:

        self._tns_admin: str = Variable.get("TNS_ADMIN")
        self._user: str = Variable.get("ADW_USER")
        self._password: str = Variable.get("ADW_PASSWORD")
        self._dsn: dict = Variable.get("ADW_SID")

        try:
            cx_Oracle.init_oracle_client(config_dir=self._tns_admin)
        except cx_Oracle.ProgrammingError:
            # 초기화 중복 발생 방지
            pass

        except Exception as e:
            logger.exception(f"{self.__class__.__name__}.__init()__ --> {e}")
            raise

        super(ADW_connection_cx_oracle, self)\
            .__init__(self._user, self._password, self._dsn)

    def cursor(self):
        return MyCursor(self)

    def get_query_from_file(self, sql_path, sql_name):
        """query를 파일로부터 불러온다."""

        try:
            sql_file = f'{sql_path}/{sql_name}'
            query = open(sql_file).read()
            return query

        except FileNotFoundError as e:
            logger.exception(f"{self.__class__.__name__}.{self.get_query_from_file.__name__} --> {e}")
            raise

class MyCursor(cx_Oracle.Cursor):

    def execute(self, query, args: tuple = ()):

        logging.info(f"Executing:, {query}")
        try:
            return super(MyCursor, self).execute(query, args)
        except cx_Oracle.DatabaseError as e:
            logger.exception(f"{self.__class__.__name__}.{self.execute.__name__} --> {e}")
            raise

    def executemany(self, query, args: dict = ()):

        logging.info(f"Executing:, {query}")
        try:
            return super(MyCursor, self).executemany(query, args)
        except cx_Oracle.DatabaseError as e:
            logger.exception(f"{self.__class__.__name__}.{self.executemany.__name__} --> {e}")
            raise

# class ADW_connection_sqlalchemy:

#     def __init__(self) -> None:

#         self._tns_admin: str = Variable.get("TNS_ADMIN")
#         self._user: str = Variable.get("ADW_USER")
#         self._password: str = Variable.get("ADW_PASSWORD")
#         self._dsn: dict = Variable.get("ADW_SID")

#         uri = f'oracle+cx_oracle://{self._user}:{self._password}@{self._dsn}'
#         self._conn = create_engine(uri) 

#     def SQL_to_dataframe(self, sql):
#         return read_sql(sql, con=self._conn)  

#     def dataframe_to_table(self, df, table_name, if_exists='replace'):
#         """
#          - 함수 설명 - 
#         1. object 타입을 VARCHAR형으로 변경하여 업로드하여 속도 개선
#         2. VARCHAR(df[c].str.len().max()) ->  VARCHAR(가장 긴행의 길이로 설정)
#         3. VARCHAR는 오라클에서 자동으로 VARCHAR2로 인식
#         """
#         try:
#             dtyp = {c:types.VARCHAR(df[c].str.len().max() if df[c].str.len().max() > 0 else 1) 
#                     for c in df.columns[df.dtypes == 'object'].tolist()}

#             # if_exists = ['replace', 'append']
#             df.to_sql(table_name, 
#                       con=self.engine.connect(),
#                       if_exists=if_exists,
#                       index=False,
#                       dtype=dtyp)    
#             print(f"테이블명: <{table_name}> 생성 및 업데이트 완료")
#         except Exception as e:       
#             print(e)
#             pass
#     pass


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

