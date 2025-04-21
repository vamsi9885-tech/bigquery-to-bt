from lumi.dataprocCreateClusterOperator import DataprocCreateClusterOperator
from lumi.dataprocSubmitJobOperator import DataprocSubmitJobOperator
from lumi.dataprocDeleteClusterOperator import DataprocDeleteClusterOperator
from lumi.dag import DAG
from datetime import datetime, timedelta
from google.cloud import bigtable

# parameters
# project_id = "axp-lumid"  # project id
gear_project_id = "prj-d-gbl-gar-epgear"  # project id
bt_project_name = "prj-d-lumi-bt"  # lumi bigtable project
cluster_name = "gear-bq-bt-cluster-1"
instance_id = "lumiplfeng-decppgusbt241030202635"  # Bigtable instance id
bt_table_name = "gear_cars_test1"  # Bigtable table name
dataset_id = "temp"  # Bigquery Dataset
bq_table_name = "cars_logs"  # Bigquery table name
pyspark_uri = "gs://prj-d-gbl-gar-epgear/gear_hist_finapi/sample_testing/testing_akbehe/beam.py"
# pyspark_bq_connector= "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"
pyspark_bq_connector = "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.27.0.jar"
json_file = "gear_hist_finapi/sample_testing/bq2bt_otl_mapping/cars_log.json"
row_key = "Request_ID"

# job object
PYSPARK_JOB = {
    "main_python_file_uri": pyspark_uri,
    "jar_file_uris": [pyspark_bq_connector],
    "args": [
        # f"--project_id={project_id}",
        f"--gear_project_id={gear_project_id}",
        f"--bt_project_name={bt_project_name}",
        f"--instance_id={instance_id}",
        f"--bt_table_name={bt_table_name}",
        f"--bq_table_name={bq_table_name}",
        f"--dataset_id={dataset_id}",
        f"--json_file={json_file}",
        f"--row_key={row_key}"
    ]
}

default_args = {
    'owner': 'airflow',
    "start_date": datetime.now(),
    "retries": 1,
    'retry_delay': timedelta(minutes=1)
}
# dag name
DAG_ID = "gear_bq_to_bt"
# dag
with DAG(dag_id=DAG_ID, default_args=default_args, schedule_interval=None) as dag:
    # Create Cluster
    create_cluster = DataprocCreateClusterOperator(task_id="create_cluster", cluster_name=cluster_name, num_workers=2,
                                                   init_actions_uris=["gs://prj-d-gbl-gar-epgear/gear_hist_finapi/sample_testing/testing_akbehe/ins_py_pkg.sh"],
                                                   metadata={'PIP_PACKAGES': 'apache-beam[gcp] google-cloud-bigtable pyarrow'},
                                                   timeout = 1800 , idle_delete_ttl=1200)
    # Execute the dataproc job
    bigtable_bigquery = DataprocSubmitJobOperator(task_id="bq_to_bt", pyspark_job=PYSPARK_JOB, retries=0)
    # Delete Cluster
    delete_cluster = DataprocDeleteClusterOperator(task_id="delete_cluster", cluster_name=cluster_name)

    # Order of Execution
    create_cluster >> bigtable_bigquery >> delete_cluster
