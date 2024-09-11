import time

from airflow import DAG
from datetime import datetime
from airflow.decorators import task

with DAG(
    dag_id='perfdag2',
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:

    @task
    def task1():
        time.sleep(300)
        return 1

    for i in range(1000):
        task1.override(task_id=f'task{i}')()
