# Orchestration recipes
Collection of data and analytics workflow patterns for Dagster or Airflow.

## Motivation

This repository demonstrates practical integration patterns for modern data orchestration tools, with a focus on three aspects:

### 1. Spark integration with orchestrators
Both [dagster-pipeline-with-spark/](dagster-pipeline-with-spark/) and [airflow-pipeline-with-spark/](airflow-pipeline-with-spark/) show how to integrate PySpark with orchestration frameworks. Each fetches data from the [Star Wars API](https://swapi.dev/) and transforms it using Spark.

### 2. Dagster vs Airflow comparison
Based on the two example pipeline implementations, my personal view is that Dagster is the better framework. The following variants of the same asset help illustrate why.

```python
# Dagster
@asset(io_manager_key="pyspark_io_manager")  # delegate I/O
def raw_planets(swapi: SWAPIResource, pyspark: PySparkResource) -> DataFrame:
    res = swapi.fetch("planets")  # injected external dependency
    return pyspark.get_session().createDataFrame(res)

# Airflow
raw_planets_asset = Asset("pyspark-data/raw_planets.parquet")  # path boilerplate

...

@task.pyspark(outlets=[raw_planets_asset])  # outlets boilerplate
def raw_planets(spark: SparkSession):
    res = fetch_swapi("planets")  # tightly coupled external dependency
    df = spark.createDataFrame(res)
    df.write.mode("overwrite").parquet("pyspark-data/raw_planets.parquet")  # I/O boilerplate
```

Concretely, the above observations show how Dagster provides the following.

1. **Less boilerplate**: as indicated by inline comments.
2. **Better data governance**: delegating path handling and I/O (reading/writing) operations provides a means to consistently handle data governance questions. For example, if the path is under a "confidential" prefix, an IO-manager can be implemented to save it accordingly.
3. **Weaker coupling to external dependencies**: with external dependencies handled by injected objects, pipelines become less tied to chosen tools and infrastructure etc. This is best exemplified by the test code below.
4. **Superior testability**: injecting fake variants of external dependency objects lets us test the pipeline without worrying about infrastructure. With Airflow, this requires mocking/patching things like the `fetch_swapi` call and/or management of custom patterns. Such approaches are possible but burdensome.
    ```python
    class FakeSWAPIResource(SWAPIResource):  # a proper subclass of the resource...
        def fetch(self, endpoint: str) -> list[dict]:
            return [{"name": "Luke Skywalker", ...}]  # ... that returns fake data...

    result = materialize(
        assets=[raw_people, clean_people, homeworlds],
        resources={"swapi": FakeSWAPIResource(), ...}  # ... lets us replace the input...
    )
    assert result.asset_value("homeworlds").toPandas().equals(expected_data)  # ... in order to check that we get the expected output
    ```

### 3. Validated File Submission Pattern
Background: data engineering workflows for analytics commonly require a means to combine manual processing steps, such as generation of data in a Jupyter Notebook or Excel, with automated pipelines.

My recommended way of achieving this is demonstrated in [submit-file-to-dagster/](submit-file-to-dagster/) and [submit-file-to-airflow/](submit-file-to-airflow/). It consists of the following components.
- In the orchestrator: an asset definition representing submitted files.
- In a utility package associated with the orchestrator: a function for submission of a file.
- Within the submission function: a validation step.

The validation step is highly useful since it imposes quality restrictions on any data submitted from lower quality code (e.g. Jupyter) for handling by higher quality code (the orchestrator), with instant feedback. Read the exemplifying notebooks for further understanding.

## Requirements
- Spark installation
- [direnv](https://direnv.net/docs/installation.html) (this is to set the environment variables of the .envrc files, which can be done in other ways)
- [uv](https://docs.astral.sh/uv/getting-started/installation/)

## Structure

Each project is self-contained with its own:
- `README.md` - Project-specific documentation and running instructions
- `pyproject.toml` - Python dependencies
- `uv.lock` - Locked dependencies
- `.venv/` - Virtual environment (gitignored)
- Project-specific data directories (gitignored)
