from airflow.decorators import dag, task
from pendulum import datetime
import duckdb
import pandas as pd

@dag(start_date=datetime(2023, 6, 1), schedule=None, catchup=False)
def duckdb_tutorial_dag_1():
    @task
    def create_pandas_df():
        # Buat DataFrame contoh
        ducks_in_my_garden_df = pd.DataFrame({"colors": ["blue", "red", "yellow"], "numbers": [2, 3, 4]})
        return ducks_in_my_garden_df

    @task
    def create_duckdb_table_from_pandas_df(ducks_in_my_garden_df):
        # Koneksi ke DuckDB (file lokal atau in-memory)
        conn = duckdb.connect("include/my_garden_ducks.db")
        conn.sql("CREATE TABLE IF NOT EXISTS ducks_garden AS SELECT * FROM ducks_in_my_garden_df;")
        sets_of_ducks = conn.sql("SELECT numbers FROM ducks_garden;").fetchall()
        for ducks in sets_of_ducks:
            print("quack " * ducks[0])

    create_duckdb_table_from_pandas_df(ducks_in_my_garden_df=create_pandas_df())

duckdb_tutorial_dag_1()