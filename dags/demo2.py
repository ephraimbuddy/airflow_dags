from airflow import DAG
from airflow.models import Variable
from datetime import datetime
from airflow.providers.standard.operators.bash import BashOperator




with DAG(dag_id="demo",start_date=datetime.now(datetime.timezone.utc),
    schedule='@daily'):
    total_var = Variable.get("total_var")
    for i in range(int(total_var)):
        task = BashOperator(task_id=f"task_{i}", bash_command=f"echo Task {i}")
    

