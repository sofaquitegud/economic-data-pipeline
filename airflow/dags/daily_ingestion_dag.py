# Import libraries
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

PIPELINE_PATH = "/opt/airflow/pipeline"
DBT_PROJECT_DIR = f"{PIPELINE_PATH}/dbt/economic_data_pipeline"
DBT_PROFILES_DIR = "/opt/airflow/.dbt"

with DAG(
    dag_id="economic_data_pipeline",
    default_args=default_args,
    description="Daily ingestion and transformation of Malaysian economic data",
    schedule_interval="0 6 * * *",  # Daily at 6 AM
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["economic", "malaysia", "etl"],
) as dag:

    # SCHEMA INITIALIZATION
    def init_schema():
        """Ensure bronze schema and tables exist."""
        import sys

        sys.path.insert(0, PIPELINE_PATH)
        from src.init_schema import ensure_schema

        ensure_schema()

    schema_init = PythonOperator(
        task_id="init_schema", python_callable=init_schema, retries=1
    )

    # INGESTION TASKS
    with TaskGroup(
        "ingestion", tooltip="Ingest data from data.gov.my"
    ) as ingestion_group:

        ingest_gdp = BashOperator(
            task_id="gdp",
            bash_command=f"cd {PIPELINE_PATH} && python -m src.ingestion.gdp_ingestion",
        )

        ingest_cpi = BashOperator(
            task_id="cpi",
            bash_command=f"cd {PIPELINE_PATH} && python -m src.ingestion.cpi_ingestion",
        )

        ingest_labour = BashOperator(
            task_id="labour",
            bash_command=f"cd {PIPELINE_PATH} && python -m src.ingestion.labour_ingestion",
        )

        ingest_exchange = BashOperator(
            task_id="exchange_rates",
            bash_command=f"cd {PIPELINE_PATH} && python -m src.ingestion.exchange_rate_ingestion",
        )

        ingest_population = BashOperator(
            task_id="population",
            bash_command=f"cd {PIPELINE_PATH} && python -m src.ingestion.population_ingestion",
        )

    # TRANSFORMATION TASKS
    with TaskGroup(
        "transformation", tooltip="Bronze -> Silver -> Gold (dbt)"
    ) as transform_group:

        setup_dbt = BashOperator(
            task_id="setup_dbt",
            bash_command=(
                f"mkdir -p {DBT_PROJECT_DIR}/logs {DBT_PROJECT_DIR}/target && "
                f"chmod -R 777 {DBT_PROJECT_DIR}/logs {DBT_PROJECT_DIR}/target || true"
            ),
        )

        dbt_build = BashOperator(
            task_id="dbt_build",
            bash_command=(
                f"cd {PIPELINE_PATH} && "
                f"dbt build "
                f"--project-dir {DBT_PROJECT_DIR} "
                f"--profiles-dir {DBT_PROFILES_DIR} "
                f"--log-path /tmp/dbt.log "
                f"--target-path /tmp/dbt_target"
            ),
        )

        setup_dbt >> dbt_build

    # DATA QUALITY CHECK
    def run_data_quality_checks():
        """Run data quality validators on dbt-built gold models."""
        import sys
        import os

        sys.path.insert(0, PIPELINE_PATH)

        from src.quality.validators import (
            validate_gdp,
            validate_cpi,
            validate_labour,
            validate_exchange_rates,
            validate_population,
        )
        from config.database import get_engine
        import pandas as pd

        engine = get_engine()

        base_schema = os.getenv("DBT_SCHEMA", "analytics")
        gold_schema = f"{base_schema}_gold"

        checks = {
            "gdp_trends": validate_gdp,
            "cpi_trends": validate_cpi,
            "labour_summary": validate_labour,
            "exchange_rate_analytics": validate_exchange_rates,
            "population_trends": validate_population,
        }

        failures = []
        for table, validator in checks.items():
            df = pd.read_sql(f"SELECT * FROM {gold_schema}.{table}", engine)
            report = validator(df)
            if not report["passed"]:
                failures.append(f"{report['dataset']}: {report['issues']}")

        if failures:
            raise ValueError(f"Quality checks failed: {'; '.join(failures)}")

        print("All data quality checks passed!")

    quality_check = PythonOperator(
        task_id="data_quality_check", python_callable=run_data_quality_checks
    )

    # TASK DEPENDENCIES
    schema_init >> ingestion_group >> transform_group >> quality_check
