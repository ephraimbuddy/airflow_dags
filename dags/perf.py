import os

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


def dag_success_alert(context):
    os.mkdir("/tmp/callback_dag2")
    with open("/tmp/callback_dag2/success.txt", "w") as f:
        f.write("task failed 1")

def dag_success_alert2(context):
    os.mkdir("/tmp/callback_dag22")
    with open("/tmp/callback_dag22/success.txt", "w") as f:
        f.write("task failed 2")

def dag_success_alert3(context):
    os.mkdir("/tmp/callback_dag23")
    with open("/tmp/callback_dag23/success.txt", "w") as f:
        f.write("task failed 3")

with DAG(
    "callback_dag2",
    schedule=None,
    start_date=(pendulum.datetime(2024, 12, 1, tz="UTC")),
):
    BashOperator(
        task_id="extract",
        bash_command="touch 'hello world' && date",
        on_failure_callback=dag_success_alert,
        cwd=".",
    )

    BashOperator(
        task_id="transform",
        bash_command="sleep 1",
        cwd=".",
        on_failure_callback=dag_success_alert3,
    )

    BashOperator(
        task_id="load",
        bash_command="true",
        cwd=".",
        on_failure_callback=dag_success_alert3,
    )
