from airflow import DAG
from datetime import datetime
from airflow.providers.standard.operators.bash import BashOperator
from module import utils


utils.traceback.print_exc()


with DAG(dag_id="demo",start_date=datetime(2025, 5, 1),
    schedule='@daily'):
    
    sleep = BashOperator(task_id="sleep_1", bash_command="sleep 600")
    hello = BashOperator(task_id="hello_1", bash_command="echo 'Hello'")
    astronomer = BashOperator(task_id="astronomer_1", bash_command="echo 'Astonomer'")

    sleep >> hello >> astronomer

