from airflow.decorators import dag, task
from pendulum import datetime
from duckdb_provider.hooks.duckdb_hook import DuckDBHook
import pandas as pd

@dag(start_date=datetime(2023, 6, 1), schedule=None, catchup=False)
def duckdb_provider_example():
    @task
    def query_duckdb():
        hook = DuckDBHook.get_hook('my_local_duckdb_conn')
        conn = hook.get_conn()
        df = conn.execute("SELECT * FROM ducks_garden").fetchdf()
        print(df)

    query_duckdb()

duckdb_provider_example()