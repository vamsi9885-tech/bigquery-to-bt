# airflow
from lumi.dag import DAG
from lumi.dataFlowJavaOperator import DataFlowJavaOperator
import os

DATAFLOW_JOB = {"jar": "/opt/airflow/mount/testDataflow-bundled-0.0.1-SNAPSHOT.jar",  #gcp path of jar file 
                "job_class": "com.aexp.ngbd.testDataflow.CsvBigtable", #Job Class
                "job_name": "test-load-bigtable-via-lumisdk",  #dataflow job name
                "pipeline_options": {
                    'input': "/opt/airflow/mount/input.csv",  #gcp bucket path of input csv file
                    'bigtableProjectId': "lumi-local",  #usecase project name 
                    'bigtableInstanceId': "fake_instance",   #Bigtable instance ID 
                    'bigtableTableId': "test_table",  #Bigtable name
                    'bigtableColumnFamily': "cf1",  #Bigtable Column Family 
                    'bigtableRowKey': "connected_cell#connected_wifi#os_build",  #Bigtable Row Keys 
                    'bigtableColumns': "connected_cell,connected_wifi,os_build"  #Bigtable Column name(s)
                },
                }

# define your dag
with DAG(dag_id="gear_bq_to_bt_java", #DAG Name 
         schedule_interval=None
         ) as dag:
    submit_dataflow_job = DataFlowJavaOperator(task_id="submit_dataflow_job",
                                               dataflow_job=DATAFLOW_JOB)