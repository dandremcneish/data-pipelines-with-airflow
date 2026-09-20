# Data Pipelines with Airflow

An Apache Airflow pipeline that stages event data from S3, loads it into a Redshift star schema, and validates every load with automated data-quality checks before it's trusted downstream. The pipeline runs as a set of four custom operators wired into a single DAG rather than relying on Airflow's built-in operators, so the staging, loading, and validation logic is explicit and easy to reason about.

## Architecture

```
S3 (raw JSON events)
   │
   ▼
StageToRedshiftOperator   →  stages log and song event data into Redshift staging tables
   │
   ▼
LoadFactOperator          →  builds the songplays fact table from staged data
   │
   ▼
LoadDimensionOperator     →  builds the user, song, artist, and time dimension tables (parallel)
   │
   ▼
DataQualityOperator       →  runs row-count and null checks; fails the DAG loudly if a check doesn't pass
```

## The four custom operators

- **`StageToRedshiftOperator`** (`plugins/operators/stage_redshift.py`) — builds and runs a SQL `COPY` statement to load JSON-formatted files from S3 into a Redshift staging table, with support for timestamped, execution-date-based file paths so backfills pull the right data.
- **`LoadFactOperator`** (`plugins/operators/load_fact.py`) — takes a SQL statement and target table and loads it in append-only mode, since fact tables should grow, not get truncated.
- **`LoadDimensionOperator`** (`plugins/operators/load_dimension.py`) — same idea, but supports a truncate-insert pattern so dimension tables can be safely rebuilt on each run.
- **`DataQualityOperator`** (`plugins/operators/data_quality.py`) — runs a list of SQL-based test cases against expected results after the load; raises and fails the task (with retries) if any check doesn't pass, rather than letting bad data flow downstream silently.

All four share a SQL helper class (`plugins/helpers/sql_queries.py`) that holds the actual transformation statements, so the operators stay focused on execution mechanics.

## DAG configuration

- No dependency on past runs, catchup disabled
- Tasks retry 3 times on failure, 5 minutes apart
- No email on retry
- Dimension table loads run in parallel once staging completes, then the data-quality checks run last, after every table is loaded

## Running it locally

Requires [Docker Desktop](https://www.docker.com/products/docker-desktop/).

```bash
docker-compose up -d
```

Visit `http://localhost:8080` once the containers are up (default Airflow UI login: `airflow` / `airflow`). Under **Admin → Connections**, add an `aws_credentials` connection and a `redshift` connection, then start the Redshift cluster from the AWS console before triggering the DAG.

## Repo layout

```
dags/
  final_project.py          # DAG definition and task dependencies
plugins/
  operators/
    stage_redshift.py
    load_fact.py
    load_dimension.py
    data_quality.py
  helpers/
    sql_queries.py
```
