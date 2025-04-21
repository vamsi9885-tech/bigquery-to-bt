
from airflow import models
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

PROJECT_ID = 'your-gcp-project-id'
REGION = 'us-central1'
BUCKET = 'your-gcs-bucket'
CLUSTER_NAME = 'bq-bigtable-cluster'
GCS_PARQUET_PATH = f'gs://{BUCKET}/parquet-output/'
SPARK_JOB_PATH = f'gs://{BUCKET}/spark_job/bq_to_gcs_parquet.py'
BEAM_JOB_PATH = f'gs://{BUCKET}/beam_job/beam_to_bigtable.py'

with models.DAG(
    dag_id='bq_to_bigtable_pipeline',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    default_args={'retries': 1, 'retry_delay': timedelta(minutes=5)},
    description='Extracts from BQ to GCS using Spark, then loads into Bigtable using Beam',
    tags=['dataproc', 'beam', 'bigquery', 'bigtable'],
) as dag:

    submit_spark_job = DataprocSubmitJobOperator(
        task_id='submit_spark_job',
        job={
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": SPARK_JOB_PATH,
                "args": [GCS_PARQUET_PATH]
            },
        },
        region=REGION,
        project_id=PROJECT_ID,
    )

    submit_beam_job = DataprocSubmitJobOperator(
        task_id='submit_beam_job',
        job={
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": BEAM_JOB_PATH,
                "args": [GCS_PARQUET_PATH]
            },
        },
        region=REGION,
        project_id=PROJECT_ID,
    )

    submit_spark_job >> submit_beam_job
