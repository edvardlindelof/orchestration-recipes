from dagster import (asset, Definitions)
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, regexp_replace

from dagster_resources import PySparkResource, PySparkIOManager, SWAPIResource


@asset(io_manager_key="pyspark_io_manager")
def raw_people(swapi: SWAPIResource, pyspark: PySparkResource) -> DataFrame:
    res = swapi.fetch("people")
    return pyspark.get_session().createDataFrame(res)

@asset(io_manager_key="pyspark_io_manager")
def raw_planets(swapi: SWAPIResource, pyspark: PySparkResource) -> DataFrame:
    res = swapi.fetch("planets")
    return pyspark.get_session().createDataFrame(res)

@asset(io_manager_key="pyspark_io_manager")
def clean_people(raw_people: DataFrame) -> DataFrame:
    df = raw_people

    # Replace unknown values with None
    unknown_values = ["unknown", "none", "null", "na", ""]
    list_columns = {"films", "species", "vehicles", "starships"}
    for col_name in df.columns:
        if col_name in list_columns:
            continue
        df = df.withColumn(
            col_name,
            when(col(col_name).isin(unknown_values), None).otherwise(col(col_name))
        )

    df = df.withColumn("height", col("height").cast("int"))
    df = df.withColumn(
        "mass",
        regexp_replace(col("mass"), ",", "").cast("double")
    )
    df = df.withColumn(
        "birth_year",
        regexp_replace(col("birth_year"), "BBY", "").cast("double").cast("int")
    )

    return df

@asset(io_manager_key="pyspark_io_manager")
def clean_planets(raw_planets: DataFrame) -> DataFrame:
    df = raw_planets

    df = df.withColumn("rotation_period", col("rotation_period").cast("int"))
    # TODO there is more cleaning to do here!

    return df

@asset(io_manager_key="pyspark_io_manager")
def homeworlds(clean_people: DataFrame, clean_planets: DataFrame) -> DataFrame:
    return clean_people.select("name", "homeworld", "mass").join(
        clean_planets.select("name", "url").withColumnRenamed("name", "homeworld_name"),
        clean_people["homeworld"] == clean_planets["url"],
        how="left"
    )


pyspark_resource = PySparkResource(app_name="my-spark-app")

defs = Definitions(
    assets=[raw_people, raw_planets, clean_people, clean_planets, homeworlds],
    resources={
        "pyspark": pyspark_resource,
        "pyspark_io_manager": PySparkIOManager(pyspark_resource=pyspark_resource),
        "swapi": SWAPIResource(),
    },
)