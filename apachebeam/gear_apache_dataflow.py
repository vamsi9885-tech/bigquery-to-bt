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

def load_json(gear_project_id, json_file):
    logging.info(f"Loading JSON file {json_file}")
    storage_client = storage.Client()
    bucket = storage_client.bucket(gear_project_id)
    blob = bucket.blob(json_file)
    str_json = blob.download_as_text()
    dict_results = json.loads(str_json)
    logging.info("JSON file loaded Successfully")
    return dict_results

gear_project_id = "prj-d-gbl-gar-epgear"
json_file = "/gear_hist_finapi/sample_testing/cars_logs/tableMapping.json"
json_content = load_json(gear_project_id , json_file ) 

DATAFLOW_JOB = {"jar": "gs://prj-d-gbl-gar-epgear/gear_hist_finapi/sample_testing/testing_akbehe/testDataflow-bundled-0.0.1-SNAPSHOT.jar",  #gcp path of jar file 
                "job_class": "com.aexp.ngbd.testDataflow.CsvBigtable", #Job Class
                "job_name": "cars-logs-bigtable-via-lumisdk",  #dataflow job name
                "pipeline_options": {
                    'input': "gs://prj-d-gbl-gar-epgear/gear_hist_finapi/sample_testing/cars_logs/cars_logs_000000000000.csv",  #gcp bucket path of input csv file
                    'bigtableProjectId': "prj-d-lumi-bt",  #usecase project name 
                    'bigtableInstanceId': "lumiplfeng-decppgusbt241030202635",   #Bigtable instance ID 
                    'bigtableTableId': "gear_cars_logs_apachebeam",  #Bigtable name
                    'bigtableColumnFamily': "cf1",  #Bigtable Column Family 
                    'bigtableRowKey': json_content.get("Rowkey"),  #Bigtable Row Keys 
                    'bigtableColumns': json_content.get("columnNames"),  #Bigtable Column name(s)
                    'tableMapping' : json_content.get("TableMapping")
                },
                }


# define your dag
with DAG(dag_id="gear_bq_to_bt_java", #DAG Name 
         schedule_interval=None
         ) as dag:
    submit_dataflow_job = DataFlowJavaOperator(task_id="submit_dataflow_job",
                                               dataflow_job=DATAFLOW_JOB)