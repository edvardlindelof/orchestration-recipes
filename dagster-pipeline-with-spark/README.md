# Dagster Pipeline with PySpark

Dagster pipeline using PySpark to fetch and transform Star Wars API data.

## Features

- Fetches data from SWAPI (Star Wars API)
- Processes people and planets data using PySpark
- Cleans and transforms data
- Joins datasets to show homeworld information

## Run

```bash
uv run dagster dev -f dagster_defs.py
```

Then open the Dagster UI (typically at http://localhost:3000) to materialize assets.

## Test

```bash
uv run pytest
```
