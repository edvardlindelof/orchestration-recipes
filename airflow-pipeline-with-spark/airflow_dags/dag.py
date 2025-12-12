# airflow
import requests

from datetime import datetime
from airflow.decorators import dag, task
from airflow.sdk import Asset
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

raw_planets_asset = Asset("pyspark-data/raw_planets.parquet")
clean_planets_asset = Asset("pyspark-data/clean_planets.parquet")


def fetch_swapi(endpoint: str) -> list[dict]:
    response = requests.get(f"https://swapi.dev/api/{endpoint}/")
    response.raise_for_status()
    return response.json()["results"]


@task.pyspark(outlets=[raw_planets_asset])
def raw_planets(spark: SparkSession):
    res = fetch_swapi("planets")
    df = spark.createDataFrame(res)
    df.write.mode("overwrite").parquet("pyspark-data/raw_planets.parquet")

@task.pyspark(inlets=[raw_planets_asset], outlets=[clean_planets_asset])
def clean_planets(spark: SparkSession):
    df = spark.read.parquet("pyspark-data/raw_planets.parquet")
    df = df.withColumn("rotation_period", col("rotation_period").cast("int"))
    df.write.mode("overwrite").parquet("pyspark-data/clean_planets.parquet")

# TODO
#
# @task.pyspark(inlets=..., outlets=...)
# def raw_people(spark: SparkSession):
#     ...

# @task.pyspark(inlets=..., outlets=...)
# def clean_people(spark: SparkSession):
#     ...

# @task.pyspark(inlets=..., outlets=...)
# def homeworlds(spark: SparkSession):
#     ...


@dag(dag_id="pyspark_pipeline", schedule=None, start_date=datetime(2024, 1, 1), catchup=False)
def pyspark_pipeline():
    raw_planets() >> clean_planets()

pyspark_pipeline()
