
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

import pendulum


PROJECT_DIR = "/opt/airflow/project"
DBT_DIR = f"{PROJECT_DIR}/dbt"
DBT = "/opt/dbt-venv/bin/dbt"
PROFILES_DIR = f"{PROJECT_DIR}/docker"

with DAG(
    dag_id="azure_sql_snowflake_medallion",
    description="Ingestão Azure SQL, transformação dbt Silver e Gold",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Sao_Paulo"),
    schedule="0 6 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(hours=2),
    },
    tags=["azure-sql", "snowflake", "dbt", "medallion"],
) as dag:

    ingest_bronze = BashOperator(
        task_id="ingest_bronze",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python -m src.ingestion.bronze_ingestion --all"
        ),
        execution_timeout=timedelta(hours=1),
    )

    validate_dbt_connection = BashOperator(
        task_id="validate_dbt_connection",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"{DBT} debug --profiles-dir {PROFILES_DIR}"
        ),
    )

    build_silver = BashOperator(
        task_id="build_silver",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"{DBT} run --select path:models/silver "
            f"--profiles-dir {PROFILES_DIR}"
        ),
    )

    build_gold = BashOperator(
        task_id="build_gold",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"{DBT} run --select path:models/gold "
            f"--profiles-dir {PROFILES_DIR}"
        ),
    )

    test_dbt = BashOperator(
        task_id="test_dbt",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"{DBT} test --profiles-dir {PROFILES_DIR}"
        ),
    )

    (
        ingest_bronze
        >> validate_dbt_connection
        >> build_silver
        >> build_gold
        >> test_dbt
    )
