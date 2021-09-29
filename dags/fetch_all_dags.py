import os
from airflow.models import DagBag

path = '/opt/airflow/dags/'
dags_dir = ['test_project', 'collaboration_project']

for dir in dags_dir:
    dag_bag = DagBag(os.path.expanduser(path + dir))

    if dag_bag:
        for dag_id, dag in dag_bag.dags.items():
            globals()[dag_id] = dag