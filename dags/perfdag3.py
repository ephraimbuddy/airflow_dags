from datetime import datetime
import time
from airflow import DAG
from airflow.operators.bash import BashOperator



with DAG(dag_id="waity3", start_date=datetime(2021, 1, 1)):
    starting_task = BashOperator(task_id="starting_task", bash_command="echo 1")
    for i in range(0, 1 * 1000):
        task = BashOperator(task_id=f"task_{i}", bash_command="sleep 1")
        starting_task >> task
        for j in [1,2,3]:
            task >> BashOperator(task_id=f"task_{i}_{j}", bash_command="sleep 1")
