
import datetime
import os
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators import dataproc


input_file = "gs://dataproc-staging-asia-southeast2-537169427832-9rlydjvh/google-cloud-dataproc-metainfo/f0bb62b5-0e85-4a7c-9fa0-c407c0582d85/cluster-awoy-m/ws2_data.csv"
PYSPARK_URI = "gs://dataproc-staging-asia-southeast2-537169427832-9rlydjvh/notebooks/jupyter/process-awoy-airflow-py3.py"
output_file = "gs://processed_awoy"
CLUSTER_NAME="awoy-cluster-test-auto"
REGION="asia-southeast1"
PROJECT_ID ="superb-receiver-344810"

yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

default_dag_args = {
    # Setting start date as yesterday starts the DAG immediately when it is
    # detected in the Cloud Storage bucket.
    'start_date': yesterday,
    # To email on failure or retry set 'email' arg to your email and enable
    # emailing here.
    'email_on_failure': False,
    'email_on_retry': False,
    # If a task fails, retry it once after waiting at least 5 minutes
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'project_id': PROJECT_ID,
    'region': REGION,

}


CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-2",
    },
}
PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name":CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI},
 }
with DAG(
    "airflow_test_dataproc",
    start_date=days_ago(1),
    schedule_interval="@once",
    tags=["test-run"]
) as dag:

    create_cluster = dataproc.DataprocCreateClusterOperator(
        task_id="create_cluster",
        cluster_name=CLUSTER_NAME,
        region=REGION,
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        )    

    pyspark_task = dataproc.DataprocSubmitJobOperator(
        task_id="pyspark_task", job=PYSPARK_JOB, region=REGION, project_id=PROJECT_ID 
        )

    delete_cluster = dataproc.DataprocDeleteClusterOperator(
        task_id="delete_cluster", project_id=PROJECT_ID, cluster_name=CLUSTER_NAME, region=REGION
        )

create_cluster >> pyspark_task >> delete_cluster