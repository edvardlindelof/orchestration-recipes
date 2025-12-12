from pathlib import Path

from dagster import materialize
import pandas as pd

from dagster_resources import SWAPIResource, PySparkResource, PySparkIOManager
from dagster_defs import raw_people, raw_planets, clean_people, clean_planets, homeworlds


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

    pyspark_resource = PySparkResource(app_name="my-spark-test-app")
    materialization_result = materialize(
        assets=[raw_people, raw_planets, clean_people, clean_planets, homeworlds],
        resources={
            "pyspark": pyspark_resource,
            "pyspark_io_manager": PySparkIOManager(
                pyspark_resource=pyspark_resource, data_path=str(tmp_path)
            ),
            "swapi": FakeSWAPIResource(),
        },
    )
    homeworlds_data = materialization_result.asset_value("homeworlds").toPandas()

    assert homeworlds_data.equals(expected_homeworlds_data)