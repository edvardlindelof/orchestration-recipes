import requests
from pathlib import Path

from pandera.typing.pandas import DataFrame, Series
import pandera.pandas as pa


DAGSTER_GRAPHQL_URL = "http://localhost:3000/graphql"


def add_dynamic_partition(partitions_def_name: str, partition_key: str) -> None:
    response = requests.post(
        DAGSTER_GRAPHQL_URL,
        json={
            "query": """
              mutation AddDynamicPartition($def: String!, $key: String!) {
                addDynamicPartition(
                  partitionKey: $key
                  partitionsDefName: $def
                  repositorySelector: {
                    repositoryName: "__repository__"
                    repositoryLocationName: "defs.py"
                  }
                ) {
                  __typename
                  ... on AddDynamicPartitionSuccess {
                    partitionKey
                  }
                }
              }
            """,
            "variables": {"def": partitions_def_name, "key": partition_key},
        },
    )
    response.raise_for_status()
    if "Error" in (json_str := response.content.decode()):
        raise RuntimeError(json_str)


def materialize(asset_paths: list[str], partition_key: str):
    response = requests.post(
        DAGSTER_GRAPHQL_URL,
        json={
            "query": """
              mutation Materialize($paths: [String!]!, $partition: String!) {
                launchPartitionBackfill(
                  backfillParams: {
                    assetSelection: {
                      path: $paths
                    }
                    partitionNames: [$partition]
                  }
                ) {
                  __typename
                  ... on LaunchBackfillSuccess {
                    launchedRunIds
                  }
                }
              }
            """,
            "variables": {"paths": asset_paths, "partition": partition_key},
        },
    )
    response.raise_for_status()
    if "Error" in (json_str := response.content.decode()):
        raise RuntimeError(json_str)


class FilmSchema(pa.DataFrameModel):
    name: Series[str]
    lead_actor: Series[str]
    rating: Series[int] = pa.Field(ge=0, le=10)


def submit_film_file(df: DataFrame[FilmSchema], filename: str):
    """Submit film dataframe to dagster, if it passes schema validation."""
    FilmSchema.validate(df)
    df.to_csv(Path("film-files") / filename)
    add_dynamic_partition("film_file_partitions", partition_key=filename)
    materialize(asset_paths=["film_files"], partition_key=filename)
