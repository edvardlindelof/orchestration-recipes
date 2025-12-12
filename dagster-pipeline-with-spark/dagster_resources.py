import os

from dagster import (
    ConfigurableIOManager,
    ConfigurableResource,
    OutputContext,
    InputContext,
    AssetKey,
    ResourceDependency,
)
from pyspark.sql import DataFrame, SparkSession
import requests


class PySparkResource(ConfigurableResource):

    app_name: str

    def get_session(self) -> SparkSession:
        """Get or create SparkSession."""
        return SparkSession.builder.appName(self.app_name).getOrCreate()


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