from pathlib import Path
import pandas as pd
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


def load_film_file(partition_key: str) -> pd.DataFrame:
    df = pd.read_csv(Path("film-files") / partition_key)
    print("loaded df:")
    print(df)
    return df


with DAG(
    dag_id="film_files_dag",
    #start_date=datetime(2024, 1, 1),
    #catchup=False,
) as dag:
    load_task = PythonOperator(
        task_id="film_files",
        python_callable=load_film_file,
        op_kwargs={"partition_key": "{{ dag_run.conf['partition_key'] }}"},
    )
