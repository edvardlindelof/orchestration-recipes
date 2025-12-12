# Submit File to Airflow

Proof of concept for submitting external files to an Airflow orchestrator for validation and processing.

## Features

- File-based workflow for external data submission
- Data validation using Pandera
- Airflow DAG-based processing

## Run

```bash
uv sync
uv run airflow standalone
```

Visit http://localhost:8080 and enter the admin credentials from the logs.

Note: You may need to unpause the DAG with:
```bash
uv run airflow dags unpause film_files_dag
```

TODO notebook
