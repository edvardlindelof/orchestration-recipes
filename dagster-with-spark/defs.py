import os

import requests
from dagster import (
    ConfigurableIOManager,
    ConfigurableResource,
    OutputContext,
    InputContext,
    AssetKey,
    ResourceDependency,
    asset,
    Definitions
)
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
    def fetch(self, endpoint: str) -> dict:
        response = requests.get(f"https://swapi.dev/api/{endpoint}/")
        response.raise_for_status()
        return response.json()


@asset(io_manager_key="pyspark_io_manager")
def raw_people(swapi: SWAPIResource, pyspark: PySparkResource) -> DataFrame:
    res = swapi.fetch("people")
    return pyspark.get_session().createDataFrame(res["results"])

@asset(io_manager_key="pyspark_io_manager")
def raw_planets(swapi: SWAPIResource, pyspark: PySparkResource) -> DataFrame:
    res = swapi.fetch("people")
    return pyspark.get_session().createDataFrame(res["results"])

@asset(io_manager_key="pyspark_io_manager")
def clean_people(raw_people: DataFrame) -> DataFrame:
    df = raw_people

    # Replace unknown values with None
    unknown_values = ["unknown", "none", "null", "na", ""]
    for col_name in df.columns:
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
        regexp_replace(col("birth_year"), "BBY", "").cast("int")
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
    return clean_people.join(
        clean_planets,
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


# TODO test suite using something like this, pyspark IOM/Res over tmp folder and materialize(...)
class FakeSWAPIResource(SWAPIResource):
    def fetch(self, endpoint: str) -> dict:
        if endpoint == "people":
            return {}
            #return {
            #    "results": [
            #        {
            #            "name": "Luke Skywalker",
            #            "height": "172",
            #            "mass": "77",
            #            "hair_color": "blond",
        if endpoint == "planets":
            return {
                "results": [
                    {
                        "name": "Tatooine",
                        "rotation_period": "23",
                        "orbital_period": "304",
                        "diameter": "10465",
                        "climate": "arid",
                        "gravity": "1 standard",
                        "terrain": "desert",
                        "url": "http://swapi.dev/api/planets/1/"
                    }
                ]
            }
        return {"results": []}