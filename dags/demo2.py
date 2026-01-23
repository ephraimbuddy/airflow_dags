from airflow import DAG
from datetime import datetime
from airflow.providers.standard.operators.bash import BashOperator


def my_dag_success_callback(context):
    print("DAG succeeded!")

def my_dag_failure_callback(context):
    print("DAG failed!")


with DAG(dag_id="demo",start_date=datetime(2024, 1, 1),
    schedule='* * * * *', catchup=False, on_success_callback=my_dag_success_callback,
    on_failure_callback=my_dag_failure_callback,) as dag:
    
    task = BashOperator(task_id=f"task_1", bash_command=f"echo Task")
    

