import datetime
import os

from airflow import models
from pathlib import Path
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitJobOperator
)

DAG_ID = "dataproc_pyspark"
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT")
INSERT_DATA_JOB_URI = "gs://scripts-data-sandbox/medallion-tables-data-insertion/insert_data_into_medallion_comment_tables.py"
CLUSTER_NAME = "data-sandbox"
REGION = "us-central1"


default_dag_args = {
    "start_date": datetime.datetime(2021, 1, 1),
}

INSERT_DATA_PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": INSERT_DATA_JOB_URI},
}

with models.DAG(
    DAG_ID,
    schedule_interval=datetime.timedelta(days=1),
    default_args=default_dag_args,
) as dag:

    insert_data_medallion_comments_tables = DataprocSubmitJobOperator(
        task_id="insert_data_medallion_comments_tables", job=INSERT_DATA_PYSPARK_JOB, region=REGION, project_id=PROJECT_ID
    )

    insert_data_medallion_comments_tables
    