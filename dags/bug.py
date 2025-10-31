from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG
from datetime import datetime


def create_dag(dag_id_prefix: str, index: int) -> DAG:
    dag_id = f"{dag_id_prefix}_{index:03d}"
    with DAG(
        dag_id=dag_id,
        is_paused_upon_creation=False,
        schedule="@daily",
        start_date=datetime(2025, 8, 1),
        max_active_runs=6,
        catchup=True,
    ) as dag:
        previous_task = None
        #for task_index in range(1,200):
        task = BashOperator(
            task_id=f"task_",
            bash_command="sleep 500",
        )
            # if previous_task is not None:
            #     previous_task >> task
            # previous_task = task
    return dag


DAG_PREFIX = "bug0"

for i in range(1):
    globals()[f"{DAG_PREFIX}_{i:03d}"] = create_dag(DAG_PREFIX, i)
