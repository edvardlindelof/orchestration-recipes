# Submit File to Dagster

Proof of concept for submitting external files to a Dagster orchestrator for validation and processing.

## Features

- File-based workflow for external data submission
- Data validation using Pandera
- Dagster asset-based processing

## Run

```bash
uv run dagster dev -f defs.py
```

Then open the Dagster UI to interact with the file submission workflow.
TODO mention notebook
