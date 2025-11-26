import os

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


@asset(io_manager_key="pyspark_io_manager")
def raw_data(pyspark: PySparkResource):
    """Create a simple PySpark DataFrame"""
    data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
    columns = ["name", "age"]
    return pyspark.get_session().createDataFrame(data, columns)


@asset(io_manager_key="pyspark_io_manager")
def processed_data(raw_data):
    """Filter and transform the data"""
    return (
        raw_data.filter(raw_data.age > 26)
        .withColumn("age_plus_5", raw_data.age + 5)
    )


pyspark_resource = PySparkResource()

defs = Definitions(
    assets=[raw_data, processed_data],
    resources={
        "pyspark": pyspark_resource,
        "pyspark_io_manager": PySparkIOManager(pyspark_resource=pyspark_resource)
    },
)


# TODO test suite using tmp folder