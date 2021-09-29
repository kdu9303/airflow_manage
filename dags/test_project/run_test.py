# airflow에 저장된 변수 불러오기
from airflow.models import Variable
import cx_Oracle

TNS_ADMIN = Variable.get("TNS_ADMIN")
cx_Oracle.init_oracle_client(config_dir=TNS_ADMIN)
print(f"TNS_ADMIN: {TNS_ADMIN}")
connection = cx_Oracle.connect(
    user=Variable.get("ADW_USER"),
    password=Variable.get("ADW_PASSWORD"),
    dsn=Variable.get("ADW_SID"))
c = connection.cursor()
c.execute('SELECT * FROM DW.재원환자리스트 where rownum < 10')
for row in c:
    print(row)