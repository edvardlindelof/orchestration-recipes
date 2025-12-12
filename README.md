# Orchestration recipes
Collection of data and analytics workflow patterns for Dagster or Airflow.

## Projects

### 1. [dagster-pipeline-with-spark/](dagster-pipeline-with-spark/)
Dagster pipeline using PySpark to fetch and transform Star Wars API data.

### 2. [airflow-pipeline-with-spark/](airflow-pipeline-with-spark/)
Airflow pipeline using PySpark for similar data processing tasks.

### 3. [submit-file-to-dagster/](submit-file-to-dagster/)
Proof of concept for submitting external files to a Dagster orchestrator.

### 4. [submit-file-to-airflow/](submit-file-to-airflow/)
Proof of concept for submitting external files to an Airflow orchestrator.

## Structure

Each project is self-contained with its own:
- `README.md` - Project-specific documentation and running instructions
- `pyproject.toml` - Python dependencies
- `uv.lock` - Locked dependencies
- `.venv/` - Virtual environment (gitignored)
- Project-specific data directories (gitignored)
