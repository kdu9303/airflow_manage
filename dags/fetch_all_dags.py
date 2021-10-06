import os
from airflow.models import DagBag

path = '/opt/airflow/dags/'

# sub폴더 생성 후 폴더 이름을 리스트에 포함
dags_dir = ['test_project', 'collaboration_project']

for dir in dags_dir:
    dag_bag = DagBag(os.path.expanduser(path + dir))

    if dag_bag:
        for dag_id, dag in dag_bag.dags.items():
            globals()[dag_id] = dag