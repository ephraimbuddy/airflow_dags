from datetime import datetime
import time
from airflow import DAG
from airflow.decorators import task as task_decorator

def mycallback():
    print('mycallback')
    
with DAG(
    dag_id='dag1',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    on_success_callback= mycallback,
) as dag:

    @task_decorator()
    def task2():
        time.sleep(5)
        print('task2')

    task2()
