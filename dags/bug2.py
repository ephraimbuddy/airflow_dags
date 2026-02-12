from airflow.providers.standard.operators.bash import BashOperator
# from airflow.operators.bash import BashOperator
from airflow import DAG

def my_callback(context):
    print("my_callback")


with DAG(
    dag_id='bug2',
    on_success_callback=my_callback,
):
    BashOperator(task_id='task1', bash_command='sleep 800')
