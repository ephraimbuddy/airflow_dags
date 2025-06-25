from airflow import DAG
from datetime import datetime
from airflow.providers.standard.operators.bash import BashOperator

with DAG(dag_id="demo2",start_date=datetime(2025, 5, 1, 3, 28, 0),
    schedule='@daily'):
    # First run
    # sleep = BashOperator(task_id="sleep", bash_command="sleep 120")
    # hello = BashOperator(task_id="hello", bash_command="echo 'Hello'")
    # astronomer = BashOperator(task_id="astronomer", bash_command="echo 'Astonomer'")
    
    # sleep >> hello >> astronomer

    # Second run
    sleep = BashOperator(task_id="sleep", bash_command="sleep 20")
    hello = BashOperator(task_id="hello", bash_command="echo 'Hello Astronomer!!'")

    sleep >> hello
