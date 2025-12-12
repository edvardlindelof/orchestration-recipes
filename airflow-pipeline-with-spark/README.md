# Airflow Pipeline with PySpark

Airflow pipeline using PySpark for data processing tasks with the Star Wars API.

## Features

- Fetches data from SWAPI (Star Wars API)
- Processes planets data using PySpark tasks
- Demonstrates Airflow's PySpark operator

## Run

```bash
uv run airflow standalone
```

Visit http://localhost:8080 and enter the admin credentials that were printed at the top of the `airflow standalone` log.

## Test

```bash
uv run pytest
```
