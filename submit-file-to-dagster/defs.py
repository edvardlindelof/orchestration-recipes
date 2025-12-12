from pathlib import Path
import pandas as pd

from dagster import DynamicPartitionsDefinition, AssetExecutionContext, asset, Definitions


film_file_partitions = DynamicPartitionsDefinition(name="film_file_partitions")

@asset(partitions_def=film_file_partitions)
def film_files(context: AssetExecutionContext) -> pd.DataFrame:
    df = pd.read_csv(Path("film-files") / context.partition_key)
    context.log.info("loaded df:")
    context.log.info(df)
    return df

defs = Definitions(assets=[film_files])