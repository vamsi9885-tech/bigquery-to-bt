from lumi.dataprocCreateClusterOperator import DataprocCreateClusterOperator
from lumi.dataprocSubmitJobOperator import DataprocSubmitJobOperator
from lumi.dataprocDeleteClusterOperator import DataprocDeleteClusterOperator
from lumi.dag import DAG
from datetime import datetime, timedelta
from google.cloud import bigtable

# parameters
project_id = "axp-lumid"  # project id
gear_project_id = "prj-d-gbl-gar-epgear"  # gear project id
bt_project_name = "prj-d-lumi-bt"  # lumi bigtable project
cluster_name = "gear-bal-cluster-1"
instance_id = "lumiplfeng-decppgusbt241030202635"  # Bigtable instance id
bt_table_name = "gear_triumph_balance_17"  # Bigtable table name
dataset_id = "dw"  # Bigquery Dataset
bq_table_name = "triumph_balance"  # Bigquery table name
pyspark_uri = "gs://prj-d-gbl-gar-epgear/gear_hist_finapi/sample_testing/bq2bt_otl_testing/gear_balance/bal_otl1.py"
json_file = "gear_hist_finapi/sample_testing/bq2bt_otl_mapping/bal_otl1_map.json"
row_key = "cm13"

# job object
PYSPARK_JOB = {
    "main_python_file_uri": pyspark_uri,
    "jar_file_uris": ["gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar","gs://spark-lib/bigtable/spark-bigtable_2.12-0.4.0.jar"],
    "args": [
        f"--project_id={project_id}",
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
DAG_ID = "gear_bal_bt_load_catalog_testing"
# dag
with DAG(dag_id=DAG_ID, default_args=default_args, schedule_interval=None) as dag:
    # Create Cluster
    create_cluster = DataprocCreateClusterOperator(task_id="create_cluster", cluster_name=cluster_name,
                                                   num_workers=2,
                                                   idle_delete_ttl=1200)
    # Execute the dataproc job
    bigtable_bigquery = DataprocSubmitJobOperator(task_id="bq_to_bt", pyspark_job=PYSPARK_JOB, retries=0)
    # Delete Cluster
    delete_cluster = DataprocDeleteClusterOperator(task_id="delete_cluster", cluster_name=cluster_name)

    # Order of Execution
    create_cluster >> bigtable_bigquery >> delete_cluster