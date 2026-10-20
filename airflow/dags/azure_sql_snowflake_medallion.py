from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


PROJECT_DIR = "/opt/airflow/project"
DBT_DIR = f"{PROJECT_DIR}/dbt"
DBT = "/opt/dbt-venv/bin/dbt"


with DAG(
    dag_id="azure_sql_snowflake_medallion",
    description="Ingestão Azure SQL, transformação dbt Silver e Gold",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "owner": "data-engineering",
        "retries": 1,
    },
    tags=["azure-sql", "snowflake", "dbt", "medallion"],
) as dag:

    ingest_bronze = BashOperator(
        task_id="ingest_bronze",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python -m src.ingestion.bronze_ingestion --all"
        ),
    )

    validate_dbt_connection = BashOperator(
        task_id="validate_dbt_connection",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"{DBT} debug --profiles-dir /opt/airflow/project/docker"
        ),
    )

    build_silver = BashOperator(
        task_id="build_silver",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"{DBT} run --select path:models/silver "
            "--profiles-dir /opt/airflow/project/docker"
        ),
    )

    build_gold = BashOperator(
        task_id="build_gold",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"{DBT} run --select path:models/gold "
            "--profiles-dir /opt/airflow/project/docker"
        ),
    )

    test_dbt = BashOperator(
        task_id="test_dbt",
        bash_command=(
            f"cd {DBT_DIR} && "
            f"{DBT} test "
            "--profiles-dir /opt/airflow/project/docker"
        ),
    )

    (
        ingest_bronze
        >> validate_dbt_connection
        >> build_silver
        >> build_gold
        >> test_dbt
    )
