import datetime
import os

from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)

# ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "dataproc_pyspark"
# PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT")
PROJECT_ID = "mcervantes-trainings"
JOB_FILE_URI = "gs://scripts-data-sandbox/medallion-tables-creation/create_medallion_comments_tables.py"
# CLUSTER_NAME = f"cluster-{ENV_ID}-{DAG_ID}".replace("_", "-")
CLUSTER_NAME = "data-sandbox"
REGION = "us-central1"

default_dag_args = {
    "start_date": datetime.datetime(2021, 1, 1),
}

PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": JOB_FILE_URI},
}

with models.DAG(
    DAG_ID,
    schedule_interval=datetime.timedelta(days=1),
    default_args=default_dag_args,
) as dag:

    create_medallion_comments_tables = DataprocSubmitJobOperator(
        task_id="create_medallion_comments_tables", job=PYSPARK_JOB, region=REGION, project_id=PROJECT_ID
    )

    # # TEST SETUP
    # create_cluster
    # # TEST BODY
    # >> pyspark_task
    # # TEST TEARDOWN
    # >> delete_cluster

    create_medallion_comments_tables