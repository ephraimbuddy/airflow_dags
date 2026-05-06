import os

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


def dag_success_alert(context):
    os.mkdir("/tmp/callback_dag2")
    with open("/tmp/callback_dag2/success.txt", "w") as f:
        f.write("DAG has succeeded")

def dag_success_alert2(context):
    os.mkdir("/tmp/callback_dag22")
    with open("/tmp/callback_dag22/success.txt", "w") as f:
        f.write("DAG has succeeded")

def dag_success_alert3(context):
    os.mkdir("/tmp/callback_dag23")
    with open("/tmp/callback_dag23/success.txt", "w") as f:
        f.write("DAG has succeeded")

with DAG(
    "callback_dag2",
    schedule=None,
    start_date=(pendulum.datetime(2024, 12, 1, tz="UTC")),
):
    BashOperator(
        task_id="extract",
        bash_command="touch 'hello world' && date",
        on_success_callback=dag_success_alert,
        cwd=".",
    )

    BashOperator(
        task_id="transform",
        bash_command="sleep 1",
        cwd=".",
        on_success_callback=dag_success_alert3,
    )

    BashOperator(
        task_id="load",
        bash_command="true",
        cwd=".",
        on_success_callback=dag_success_alert3,
    )
