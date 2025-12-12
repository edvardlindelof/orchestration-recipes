"""WIP test. Doesn't work yet."""
from pyspark.sql import SparkSession

from airflow_dags.dag import clean_planets


def test_clean_planets():
    """Failed attempt at testing a single Airflow task."""
    spark = SparkSession.builder.appName("test").master("local").getOrCreate()

    fake_swapi_response = [
        {"name": "Tatooine", "rotation_period": "23"},
        {"name": "Alderaan", "rotation_period": "24"},
        {"name": "Naboo", "rotation_period": "26"},
    ]
    
    expected_data = [
        {"name": "Tatooine", "rotation_period": 23},
        {"name": "Alderaan", "rotation_period": 24},
        {"name": "Naboo", "rotation_period": 26},
    ]
    
    input_path = "pyspark-data/raw_planets.parquet"
    output_path = "pyspark-data/clean_planets.parquet"
    
    input_df = spark.createDataFrame(fake_swapi_response)
    input_df.write.mode("overwrite").parquet(input_path)
    
    # Because of the airlfow decorator, this does not execute the actual code
    # Need to refactor to have a chance of testing anything
    clean_planets(spark)
    
    result_df = spark.read.parquet(output_path)
    result_dicts = [row.asDict() for row in result_df.collect()]
    
    assert result_dicts == expected_data
