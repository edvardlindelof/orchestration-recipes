# airflow
from datetime import datetime
from airflow.decorators import dag, task
from airflow.sdk import Asset

#raw_asset = Asset("file:///pyspark-data/raw_data.parquet")
#processed_asset = Asset("file:///pyspark-data/processed_data.parquet")
raw_asset = Asset("pyspark-data/raw_data.parquet")
processed_asset = Asset("pyspark-data/processed_data.parquet")

@dag(dag_id="pyspark_pipeline", schedule=None, start_date=datetime(2024, 1, 1), catchup=False)
def pyspark_pipeline():
    
    @task.pyspark(outlets=[raw_asset])
    def raw_data(spark=None):
        df = spark.createDataFrame([("Alice", 25), ("Bob", 30), ("Charlie", 35)], ["name", "age"])
        df.write.mode("overwrite").parquet("pyspark-data/raw_data.parquet")

    @task.pyspark(inlets=[raw_asset], outlets=[processed_asset])
    def processed_data(spark=None):
        df = spark.read.parquet("pyspark-data/raw_data.parquet")
        (
            df.filter(df.age > 26)
            .withColumn("age_plus_5", df.age + 5)
            .write.mode("overwrite").parquet("pyspark-data/processed_data.parquet")
        )

    raw_data() >> processed_data()

pyspark_pipeline()
