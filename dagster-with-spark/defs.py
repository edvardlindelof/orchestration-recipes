import os
from pathlib import Path

import requests
from dagster import (
    ConfigurableIOManager,
    ConfigurableResource,
    OutputContext,
    InputContext,
    AssetKey,
    ResourceDependency,
    asset,
    Definitions,
    materialize
)
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, when, regexp_replace


class PySparkResource(ConfigurableResource):

    # TODO add conf fields for understanding

    def get_session(self) -> SparkSession:
        """Get or create SparkSession."""
        return SparkSession.builder.appName("DagsterPySpark").getOrCreate()


class PySparkIOManager(ConfigurableIOManager):
    """IO Manager for PySpark DataFrames stored as Parquet files."""
    
    data_path: str = "pyspark-data"

    pyspark_resource: ResourceDependency[PySparkResource]
    
    def _get_path(self, asset_key: AssetKey) -> str:
        """Construct file path from asset key."""
        return os.path.join(self.data_path, *asset_key.path) + ".parquet"
    
    def handle_output(self, context: OutputContext, obj: DataFrame) -> None:
        """Write PySpark DataFrame to Parquet."""
        path = self._get_path(context.asset_key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        obj.write.mode("overwrite").parquet(path)
    
    def load_input(self, context: InputContext) -> DataFrame:
        """Load PySpark DataFrame from Parquet."""
        path = self._get_path(context.asset_key)
        spark = self.pyspark_resource.get_session()
        df = spark.read.parquet(path)
        return df


class SWAPIResource(ConfigurableResource):
    def fetch(self, endpoint: str) -> list[dict]:
        response = requests.get(f"https://swapi.dev/api/{endpoint}/")
        response.raise_for_status()
        return response.json()["results"]


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


pyspark_resource = PySparkResource()

defs = Definitions(
    assets=[raw_people, raw_planets, clean_people, clean_planets, homeworlds],
    resources={
        "pyspark": pyspark_resource,
        "pyspark_io_manager": PySparkIOManager(pyspark_resource=pyspark_resource),
        "swapi": SWAPIResource(),
    },
)



def test_assets(tmp_path: Path):
    """Run pipeline and check that last asset has expected data."""
    class FakeSWAPIResource(SWAPIResource):
        def fetch(self, endpoint: str) -> list[dict]:
            if endpoint == "people":
                return [
                    {
                        "name": "Luke Skywalker",
                        "height": "172",
                        "mass": "77",
                        "birth_year": "19BBY",
                        "homeworld": "https://swapi.dev/api/planets/1/",
                    }
                ]
            if endpoint == "planets":
                return [
                    {
                        "name": "Tatooine",
                        "rotation_period": "23",
                        "url": "https://swapi.dev/api/planets/1/"
                    }
                ]
            return []

    expected_homeworlds_data = pd.DataFrame(
        {
            "name": ["Luke Skywalker"],
            "homeworld": ["https://swapi.dev/api/planets/1/"],
            "mass": [77.0],
            "homeworld_name": ["Tatooine"],
            "url": ["https://swapi.dev/api/planets/1/"],
        }
    )

    materialization_result = materialize(
        assets=[raw_people, raw_planets, clean_people, clean_planets, homeworlds],
        resources={
            "pyspark": PySparkResource(),
            "pyspark_io_manager": PySparkIOManager(
                pyspark_resource=PySparkResource(),
                data_path=str(tmp_path)
            ),
            "swapi": FakeSWAPIResource(),
        },
    )
    homeworlds_data = materialization_result.asset_value("homeworlds").toPandas()

    assert homeworlds_data.equals(expected_homeworlds_data), homeworlds_data.to_string() + "\n\n" + expected_homeworlds_data.to_string()