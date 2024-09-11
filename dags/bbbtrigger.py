from airflow.decorators import dag, task
from airflow.datasets import Dataset, DatasetAlias
from airflow.datasets.metadata import Metadata
from pendulum import datetime

my_alias_name = "alias-dataset-9"


@dag(
    dag_display_name="TEST Alias upstream",
    start_date=datetime(2024, 8, 1),
    schedule=None,
    catchup=False,
    tags=["Test"],
)
def dataset_alias_dynamic_test():
    @task
    def upstream_task():
        return ["a", "b"]

    @task(outlets=[DatasetAlias(my_alias_name)])
    def use_metadata(name):
        yield Metadata(
            Dataset(name),
            alias=my_alias_name,
            extra={}  # extra is NOT optional
        )

    use_metadata.expand(name=upstream_task())


dataset_alias_dynamic_test()


@dag(
    start_date=datetime(2024, 8, 1),
    schedule=[DatasetAlias(my_alias_name)],
    catchup=False,
    tags=["Test"]
)
def downstream_alias():
    @task
    def t1():
        return 0

    t1()


downstream_alias()
