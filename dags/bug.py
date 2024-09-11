from datetime import datetime
import time
from airflow import DAG
from airflow.decorators import task as task_decorator
with DAG(
    dag_id='dag1',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    @task_decorator()
    def task1():
        time.sleep(5)
        print('task1')

    @task_decorator()
    def task2():
        time.sleep(5)
        print('task2')

    task1() >> task2()

