from datetime import datetime
import time
from airflow import DAG
from airflow.operators.bash import BashOperator


class FailsFirstTimeOperator(BashOperator):
    def execute(self, context):
        if context["ti"].try_number == 1:
            raise Exception("I fail the first time on purpose to test retry delay")
        print(context["ti"].try_number)
        return super().execute(context)


with DAG(dag_id="waity2", start_date=datetime(2021, 1, 1)):
    starting_task = FailsFirstTimeOperator(task_id="starting_task", retry_delay=30, retries=1,
                                           bash_command="echo whee")
    for i in range(0, 1 * 1000):
        task = BashOperator(task_id=f"task_{i}", bash_command="sleep 300")
        starting_task >> task
        for j in [1,2,3]:
            task >> BashOperator(task_id=f"task_{i}_{j}", bash_command="sleep 1")
