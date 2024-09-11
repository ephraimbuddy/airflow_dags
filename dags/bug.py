from __future__ import annotations

from datetime import datetime

from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.datasets import Dataset
from airflow.operators.empty import EmptyOperator


dataset1 = Dataset("s3://bucket/dataset1.csv")

with DAG("emptyop", start_date=datetime(2021, 1, 1)) as dag:

    EmptyOperator(task_id="producing_task_1")
