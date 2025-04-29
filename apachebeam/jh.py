# airflow
from lumi.dag import DAG
from lumi.dataFlowJavaOperator import DataFlowJavaOperator
from google.cloud import bigtable, bigquery, storage
import logging
import json
from lumi.dataprocCreateClusterOperator import DataprocCreateClusterOperator
from lumi.dataprocSubmitJobOperator import DataprocSubmitJobOperator
from lumi.dataprocDeleteClusterOperator import DataprocDeleteClusterOperator
import os

dag_dir = os.path.dirname(os.path.abspath(__file__))
json_file = "tableMapping.json"
json_path = os.path.join(dag_dir , json_file)
cluster_name = "gear-bq-bt-cluster-1"

with open(json_path , 'r') as f:
    json_content = json.loads(f)
DATAFLOW_JOB = {"jar": "gs://prj-d-gbl-gar-epgear/gear_hist_finapi/sample_testing/testing_akbehe/testDataflow-bundled-0.0.1-SNAPSHOT.jar",  #gcp path of jar file 
                "job_class": "com.aexp.ngbd.testDataflow.CsvBigtable", #Job Class
                "job_name": "cars-logs-bigtable-via-lumisdk",  #dataflow job name
                "pipeline_options": {
                    'input': f"gs://{json_content.get('gearProjectId')}/{json_content.get('tempPath')}*.csv",  #gcp bucket path of input csv file
                    'bigtableProjectId': json_content.get("btProjectId"),  #usecase project name 
                    'bigtableInstanceId': json_content.get("btInstanceId"),   #Bigtable instance ID 
                    'bigtableTableId': json_content.get("btTableName"),  #Bigtable name
                    'bigtableColumnFamily':json_content.get("btColumnFamily"),  #Bigtable Column Family 
                    'bigtableRowKey': json_content.get("Rowkey"),  #Bigtable Row Keys 
                    'bigtableColumns': json_content.get("columnNames")  #Bigtable Column name(s)
                },
                }
PRE_PYSPARK_JOB   = {
    "main_python_file_uri": json_content.get("preDataFlowPyURI"),
    "jar_file_uris": [json_content.get("pysparkBqConnector")],
    "args": [
        # f"--project_id={project_id}",
        f"--gear_project_id={json_content.get('gearProjectId')}",
        f"--bq_table_name={json_content.get('bqTableName')}",
        f"--dataset_id={json_content.get('datasetId')}",
        f"--temp_path={json_content.get('tempPath')}"
    ]
}

POST_PYSPARK_JOB  =  {
    "main_python_file_uri": json_content.get("postDataFlowPyURI"),
    "jar_file_uris": [json_content.get("pysparkBqConnector")],
    "args": [
        # f"--project_id={project_id}",
        f"--gear_project_id={json_content.get('gearProjectId')}",
        f"--temp_path={json_content.get('tempPath')}"
    ]
}


# define your dag
with DAG(dag_id="gear_bq_to_bt_java", #DAG Name 
         schedule_interval=None
         ) as dag:
    
    create_cluster = DataprocCreateClusterOperator(task_id="create_cluster", cluster_name=cluster_name, num_workers=2,
                                                   timeout = 1800 , idle_delete_ttl=1200)
    # Execute the dataproc job
    bigtable_GCS = DataprocSubmitJobOperator(task_id="bg_to_gcs", pyspark_job=PRE_PYSPARK_JOB, retries=0)

    submit_dataflow_job = DataFlowJavaOperator(task_id="submit_dataflow_job", dataflow_job=DATAFLOW_JOB)

    GCS_deletion = DataprocSubmitJobOperator(task_id="bg_to_gcs", pyspark_job=POST_PYSPARK_JOB, retries=0)
    # Delete Cluster
    delete_cluster = DataprocDeleteClusterOperator(task_id="delete_cluster", cluster_name=cluster_name)

    # Order of Execution
    create_cluster >> bigtable_GCS >> GCS_deletion >> delete_cluster
