"""
Airflow DAG for managing real-time streaming platform workflows.
Orchestrates model retraining, quality checks, and Snowflake sync.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-platform-team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    'realtime_streaming_platform',
    default_args=default_args,
    description='Orchestrate streaming pipeline, ML retraining, and Snowflake sync',
    schedule_interval='0 */6 * * *',
    catchup=False,
    tags=['streaming', 'realtime', 'mlops', 'snowflake'],
) as dag:

    # Task 1: Run Databricks streaming job health check
    streaming_health_check = DatabricksRunNowOperator(
        task_id='streaming_health_check',
        job_id='12345',
        notebook_params={"check_type": "full"}
    )

    # Task 2: Check model drift
    def check_model_drift(**context):
        import mlflow
        client = mlflow.tracking.MlflowClient()
        latest_version = client.get_latest_versions("anomaly_detection_model")[0]
        baseline_auc = 0.95
        current_auc = float(latest_version.tags.get("test_auc", 0.90))
        
        if current_auc < baseline_auc - 0.05:
            print(f"Drift detected: {current_auc} < {baseline_auc - 0.05}")
            return "retrain_model"
        return "skip_retrain"

    drift_check = PythonOperator(
        task_id='check_model_drift',
        python_callable=check_model_drift
    )

    # Task 3: Retrain model
    retrain_model = BashOperator(
        task_id='retrain_model',
        bash_command='cd /opt/airflow/dags && python src/ml/anomaly_detection.py --mode=train',
    )

    # Task 4: Sync Gold layer to Snowflake
    sync_to_snowflake = SnowflakeOperator(
        task_id='sync_gold_to_snowflake',
        sql="""
            COPY INTO analytics.gold_events
            FROM @azure_stage/gold/events/
            FILE_FORMAT = (TYPE = PARQUET)
            PATTERN = '.*parquet';
        """,
        snowflake_conn_id='snowflake_default'
    )

    # Task 5: Data quality check on Snowflake
    quality_check = SnowflakeOperator(
        task_id='snowflake_quality_check',
        sql="""
            SELECT COUNT(*) as row_count,
                   COUNT(DISTINCT event_id) as unique_events,
                   AVG(value) as avg_value
            FROM analytics.gold_events
            WHERE ingestion_date = CURRENT_DATE();
        """,
        snowflake_conn_id='snowflake_default'
    )

    # Dependencies
    streaming_health_check >> drift_check
    drift_check >> retrain_model
    streaming_health_check >> sync_to_snowflake >> quality_check
