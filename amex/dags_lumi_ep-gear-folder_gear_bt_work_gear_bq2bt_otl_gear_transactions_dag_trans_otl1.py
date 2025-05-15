from lumi.dataprocCreateClusterOperator import DataprocCreateClusterOperator
from lumi.dataprocSubmitJobOperator import DataprocSubmitJobOperator
from lumi.dataprocDeleteClusterOperator import DataprocDeleteClusterOperator
from lumi.dag import DAG
from datetime import datetime, timedelta
from airflow.models import Variable
from google.cloud import bigtable

config = Variable.get("EPGEAR_CONFIG_SELECTOR",deserialize_json=True)


#Parameters
pyspark_uri = config['pyspark_path'] + "gear_transaction/trans_otl1.py"
json_file = config['json_path'] + "trans_otl1_map.json"

# job object
PYSPARK_JOB = {
    "main_python_file_uri": pyspark_uri,
    "jar_file_uris": ["gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar","gs://spark-lib/bigtable/spark-bigtable_2.12-0.4.0.jar"],
    "args": [
        f"--project_id={config['project_id']}",
        f"--gear_project_id={config['gear_project_id']}",
        f"--bt_project_name={config['bt_project_name']}",
        f"--instance_id={config['instance_id']}",
        f"--dataset_id={config['dataset_id']}",
        f"--json_file={json_file}"
    ]
}

default_args = {
    'owner': 'airflow',
    "start_date": datetime.now(),
    "retries": 1,
    'retry_delay': timedelta(minutes=1)
}
# dag name
DAG_ID = "gear_trans_bt_load_catalog_testing"
cluster_name = "gear-trans-cluster-2"
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