from pathlib import Path
from pandera.typing.pandas import DataFrame, Series
import pandera.pandas as pa
from airflow.api.client.local_client import Client


class FilmSchema(pa.DataFrameModel):
    name: Series[str]
    lead_actor: Series[str]
    rating: Series[int] = pa.Field(ge=0, le=10)


def submit_film_file(df: DataFrame[FilmSchema], filename: str):
    """Submit film dataframe to Airflow, if it passes schema validation."""
    FilmSchema.validate(df)
    df.to_csv(Path("film-files") / filename)
    
    client = Client()
    client.trigger_dag(
        dag_id="film_files_dag",
        conf={"partition_key": filename}
    )
