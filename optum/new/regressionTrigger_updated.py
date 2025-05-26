# Databricks notebook source
# DBTITLE 1,Installation
# MAGIC %pip install psycopg2
# MAGIC %pip install adlfs
# MAGIC %pip install azure-identity
# MAGIC %pip install azure-keyvault-secrets
# MAGIC %pip install openpyxl
# MAGIC %pip install azure-identity azure-storage-blob
# MAGIC %pip install azure-storage-blob azure-mgmt-datafactory
# MAGIC %pip install azure-monitor-query
# MAGIC %pip install azure-identity azure-mgmt-monitor
# MAGIC
# MAGIC

# COMMAND ----------

dbutils.widgets.text("environment", "qa")
env = dbutils.widgets.get("environment")
print(f"environment : {env}")

# COMMAND ----------

# DBTITLE 1,Function Calls
import os
import logging
import psycopg2
import pandas.io.sql as sqlio
from constants import get_pipeline_runs
pipeline_runs = get_pipeline_runs("hsodev-{env}-InvokingDVP-etl-pipeline", 270, env)
for run in pipeline_runs:
        if run.pipeline_name == f"hsodev-{env}-InvokingDVP-etl-pipeline" and run.parameters['FeedName'] == feed_name  :
            print(run)
            if run.status == 'Succeeded':
                count += 1
    if count >= 15 :
        display("Testcase TC_PUSH_034 Passed")
        save_output("TC_PUSH_034", test_case_desc,"PASS")
    else:
        display("Testcase TC_PUSH_034 Failed")
        save_output("TC_PUSH_034", test_case_desc,"FAIL","DVP trigger failed ")
except Exception as e:
    print(f"Testcase {test_id} Failed due to exception: {e}")
    save_output(test_id, test_case_desc, "FAIL", str(e))

# COMMAND ----------

# DBTITLE 1,TC_PUSH_038
test_case = test_cases_dict.get("TC_PUSH_038")
if test_case:
    post_test_sql = test_case.get('PostTest_SQL')
    test_id = test_case.get('TestID')
    test_file_names = test_case.get('Test_File_Names')
    landed_folder = test_case.get('Landed_Folder').format(env=env)
    test_case_desc = test_case.get('Test_Case_Description')  
    sql_file_path = f'{dbfs_testcase_path}/{post_test_sql}'
    error_sql = test_case.get("ErrorSQL")
    print(test_case_desc)
    # Call run_sql_file() function to get test result
    sql_result = run_post_test_sql(sql_file_path,test_id)
    print("Query execution status of TC_PUSH_038:", sql_result)
    try:
        if sql_result == "PASS":
            file_path = f'{test_case_path}/tc38/input_files/{test_file_names}'
            source_row_count = read_source_row_count(file_path)
            print("source_row_count: ", source_row_count)
            parquet_row_count = read_parquet_count_rows(landed_folder)
            print("parquet_row_count: ", parquet_row_count)
        if sql_result == "PASS" and source_row_count == parquet_row_count:
            display("Testcase TC_PUSH_038 Passed")
            save_output("TC_PUSH_038",test_case_desc, "PASS")
        else:
            sql_error_file_path = f'{dbfs_testcase_path}/{error_sql}'
            error=get_error_result_from_sql(sql_error_file_path)
            display("Testcase TC_PUSH_038 Failed")
            save_output("TC_PUSH_038", test_case_desc,"FAIL", error)
    except Exception as e:
        print(f"Testcase {test_id} Failed due to exception: {e}")
        save_output(test_id, test_case_desc, "FAIL", str(e))
        

# COMMAND ----------

# DBTITLE 1,TC_PUSH_39
test_case = test_cases_dict.get("TC_PUSH_039")
if test_case:
    post_test_sql = test_case.get('PostTest_SQL')
    test_id = test_case.get('TestID')
    test_file_names = test_case.get('Test_File_Names')
    landed_folder = test_case.get('Landed_Folder').format(env=env)
    test_case_desc = test_case.get('Test_Case_Description')  
    error_sql = test_case.get("ErrorSQL")
    sql_file_path = f'{dbfs_testcase_path}/{post_test_sql}'
    print(test_case_desc)
    # Call run_sql_file() function to get test result
    sql_result = run_post_test_sql(sql_file_path,test_id)
    print("Query execution status of TC_PUSH_039:", sql_result)
    try:
        if sql_result == "PASS":
            display("Testcase TC_PUSH_039 Passed")
            save_output("TC_PUSH_039",test_case_desc, "PASS")
        else:
            sql_error_file_path = f'{dbfs_testcase_path}/{error_sql}'
            error=get_error_result_from_sql(sql_error_file_path)
            display("Testcase TC_PUSH_039 Failed")
            save_output("TC_PUSH_039",test_case_desc, "FAIL", error)
    except Exception as e:
        print(f"Testcase {test_id} Failed due to exception: {e}")
        save_output(test_id, test_case_desc, "FAIL", str(e))
        

# COMMAND ----------

# DBTITLE 1,TC_PUSH_040
test_case = test_cases_dict.get("TC_PUSH_040")
if test_case:
    post_test_sql = test_case.get('PostTest_SQL')
    test_id = test_case.get('TestID')
    test_file_names = test_case.get('Test_File_Names')
    landed_folder = test_case.get('Landed_Folder').format(env=env)
    test_case_desc = test_case.get('Test_Case_Description')  
    error_sql = test_case.get("ErrorSQL")

    sql_file_path = f'{dbfs_testcase_path}/{post_test_sql}'
    print(test_case_desc)
    # Call run_sql_file() function to get test result
    sql_result = run_post_test_sql(sql_file_path,test_id)
    print("Query execution status of TC_PUSH_040:", sql_result)
    try:
        if sql_result == "PASS":
            file_path = f'{test_case_path}/tc40/input_files/{test_file_names}'

            # Read source file with multiple extension support and count rows
            source_row_count = read_source_row_count(file_path)
            print("source_row_count: ", source_row_count)

            # Read folder and count parquet files
            parquet_file_count = read_parquet_file_count(landed_folder)
            print("parquet_file_count: ", parquet_file_count)

            # Read parquet file and count rows
            parquet_row_count = read_parquet_count_rows(landed_folder)
            print("parquet_row_count: ", parquet_row_count)

        if sql_result == "PASS" and parquet_file_count == 1 and source_row_count == parquet_row_count:
            display("Testcase TC_PUSH_040 Passed")
            save_output("TC_PUSH_040", test_case_desc,"PASS")
        else:
            sql_error_file_path = f'{dbfs_testcase_path}/{error_sql}'
            error=get_error_result_from_sql(sql_error_file_path)
            display("Testcase TC_PUSH_040 Failed")
            save_output("TC_PUSH_040",test_case_desc, "FAIL", error)
    except Exception as e:
        print(f"Testcase {test_id} Failed due to exception: {e}")
        save_output(test_id, test_case_desc, "FAIL", str(e))

# COMMAND ----------

# DBTITLE 1,TC_PUSH_041
test_case = test_cases_dict.get("TC_PUSH_041")
if test_case:
    post_test_sql = test_case.get('PostTest_SQL')
    test_id = test_case.get('TestID')
    test_file_names = test_case.get('Test_File_Names')
    landed_folder = test_case.get('Landed_Folder').format(env=env)
    test_case_desc = test_case.get('Test_Case_Description')  
    error_sql = test_case.get("ErrorSQL")
    sql_file_path = f'{dbfs_testcase_path}/{post_test_sql}'
    
    print(test_case_desc)
    # Call run_sql_file() function to get test result
    sql_result = run_post_test_sql(sql_file_path,test_id)
    print("Query execution status of TC_PUSH_041:", sql_result)
    try:
        if sql_result == "PASS":
            file_path = f'{test_case_path}/tc41/input_files/{test_file_names}'

            # Read source file with multiple extension support and count rows
            source_row_count = read_source_row_count(file_path)
            print("source_row_count: ", source_row_count)

            # Read folder and count parquet files
            parquet_file_count = read_parquet_file_count(landed_folder)
            print("parquet_file_count: ", parquet_file_count)

            # Read parquet file and count rows
            parquet_row_count = read_parquet_count_rows(landed_folder)
            print("parquet_row_count: ", parquet_row_count)

        if sql_result == "PASS" and parquet_file_count == 1 and source_row_count == parquet_row_count:
            display("Testcase TC_PUSH_041 Passed")
            save_output("TC_PUSH_041",test_case_desc, "PASS")
        else:
            sql_error_file_path = f'{dbfs_testcase_path}/{error_sql}'
            error=get_error_result_from_sql(sql_error_file_path)
            display("Testcase TC_PUSH_041 Failed")
            save_output("TC_PUSH_041",test_case_desc, "FAIL", error)
    except Exception as e:
        print(f"Testcase {test_id} Failed due to exception: {e}")
        save_output(test_id, test_case_desc, "FAIL", str(e))
        

# COMMAND ----------

# DBTITLE 1,TC_PUSH_042
test_case = test_cases_dict.get("TC_PUSH_042")
if test_case:
    post_test_sql = test_case.get('PostTest_SQL')
    test_id = test_case.get('TestID')
    test_file_names = test_case.get('Test_File_Names')
    landed_folder = test_case.get('Landed_Folder').format(env=env)
    test_case_desc = test_case.get('Test_Case_Description') 
    error_sql = test_case.get("ErrorSQL") 

    sql_file_path = f'{dbfs_testcase_path}/{post_test_sql}'
    
    print(test_case_desc)
    # Call run_sql_file() function to get test result
    sql_result = run_post_test_sql(sql_file_path,test_id)
    print("Query execution status of TC_PUSH_042:", sql_result)
    try:
        if sql_result == "PASS":
            file_path = f'{test_case_path}/tc42/input_files/{test_file_names}'

            # Read source file with multiple extension support and count rows
            source_row_count = read_source_row_count(file_path)
            print("source_row_count: ", source_row_count)

            # Read folder and count parquet files
            parquet_file_count = read_parquet_file_count(landed_folder)
            print("parquet_file_count: ", parquet_file_count)

            # Read parquet file and count rows
            parquet_row_count = read_parquet_count_rows(landed_folder)
            print("parquet_row_count: ", parquet_row_count)

        if sql_result == "PASS" and parquet_file_count == 1 and source_row_count == parquet_row_count:
            display("Testcase TC_PUSH_042 Passed")
            save_output("TC_PUSH_042", test_case_desc,"PASS")
        else:
            sql_error_file_path = f'{dbfs_testcase_path}/{error_sql}'
            error=get_error_result_from_sql(sql_error_file_path)
            display("Testcase TC_PUSH_042 Failed")
            save_output("TC_PUSH_042", test_case_desc,"FAIL",error)
    except Exception as e:
        print(f"Testcase {test_id} Failed due to exception: {e}")
        save_output(test_id, test_case_desc, "FAIL", str(e))

# COMMAND ----------

# DBTITLE 1,TC_PUSH_043
test_case = test_cases_dict.get("TC_PUSH_043")
if test_case:
    post_test_sql = test_case.get('PostTest_SQL')
    test_id = test_case.get('TestID')
    test_file_names = test_case.get('Test_File_Names')
    landed_folder = test_case.get('Landed_Folder').format(env=env)
    test_case_desc = test_case.get('Test_Case_Description')  
    print(test_case_desc)
    sql_file_path = f'{dbfs_testcase_path}/{post_test_sql}'
    rerun_count_sql = test_case.get('PreTest_SQL')
    Archive_path = test_case.get('Archive_Folder')
    error_sql = test_case.get("ErrorSQL")

    # Call run_sql_file() function to get test result
    sql_result = run_post_test_sql(sql_file_path,test_id)
    print("Query execution status of TC_PUSH_043:", sql_result)
    try:
        if sql_result == "PASS":
            file_path = f'{test_case_path}/tc43/input_files/{test_file_names}'

            # Read source file with multiple extension support and count rows
            source_row_count = read_source_row_count(file_path)
            print("source_row_count: ", source_row_count)

            # Read folder and count parquet files
            parquet_file_count = read_parquet_file_count(landed_folder)
            print("parquet_file_count: ", parquet_file_count)

            # Read parquet file and count rows
            parquet_row_count = read_parquet_count_rows(landed_folder)
            print("parquet_row_count: ", parquet_row_count)

            # Get rerun count from database
            re_run_count_df = get_rerun_count(rerun_count_sql)
            re_run_count = re_run_count_df.iloc[0]['re_run_count']
            print("re_run_count_df: ", re_run_count)

            # Read archive folder using rerun count and count parquet files in archive folder 
            archive_file_count = read_parquet_file_count(Archive_path.format(re_run_count = re_run_count,env=env))
            print("archive_file_count: ", archive_file_count)

        if sql_result == "PASS" and parquet_file_count == 1 and source_row_count == parquet_row_count and archive_file_count == 1:
            display("Testcase TC_PUSH_043 Passed")
            save_output("TC_PUSH_043",test_case_desc, "PASS")
        else:
            sql_error_file_path = f'{dbfs_testcase_path}/{error_sql}'
            error=get_error_result_from_sql(sql_error_file_path)
            display("Testcase TC_PUSH_043 Failed")
            save_output("TC_PUSH_043",test_case_desc, "FAIL",error)
    except Exception as e:
        print(f"Testcase {test_id} Failed due to exception: {e}")
        save_output(test_id, test_case_desc, "FAIL", str(e))

# COMMAND ----------

# DBTITLE 1,TC_PUSH_044
test_case = test_cases_dict.get("TC_PUSH_044")
if test_case:
    post_test_sql = test_case.get('PostTest_SQL')
    test_id = test_case.get('TestID')
    test_file_names = test_case.get('Test_File_Names')
    landed_folder = test_case.get('Landed_Folder').format(env=env)
    test_case_desc = test_case.get('Test_Case_Description')  
    error_sql = test_case.get("ErrorSQL")
    sql_file_path = f'{dbfs_testcase_path}/{post_test_sql}'
    print(test_case_desc)
    # Call run_sql_file() function to get test result
    sql_result = run_post_test_sql(sql_file_path,test_id)
    print("Query execution status of TC_PUSH_044:", sql_result)
    try:
        if sql_result == "PASS":
            file_path = f'{test_case_path}/tc44/input_files/{test_file_names}'

            # Read source file with multiple extension support and count rows
            source_row_count = read_source_row_count(file_path)
            print("source_row_count: ", source_row_count)

            # Read folder and count parquet files
            parquet_file_count = read_parquet_file_count(landed_folder)
            print("parquet_file_count: ", parquet_file_count)

            # Read parquet file and count rows
            parquet_row_count = read_parquet_count_rows(landed_folder)
            print("parquet_row_count: ", parquet_row_count)

            # Archive folder check, Extracted_holding folder check,Auto Feed completion status mail check--Pending

        if sql_result == "PASS" and parquet_file_count == 1 and source_row_count == parquet_row_count:
            display("Testcase TC_PUSH_044 Passed")
            save_output("TC_PUSH_044",test_case_desc, "PASS")
        else:
            sql_error_file_path = f'{dbfs_testcase_path}/{error_sql}'
            error=get_error_result_from_sql(sql_error_file_path)
            display("Testcase TC_PUSH_044 Failed")
            save_output("TC_PUSH_044",test_case_desc, "FAIL",error)
    except Exception as e:
        print(f"Testcase {test_id} Failed due to exception: {e}")
        save_output(test_id, test_case_desc, "FAIL", str(e))

# COMMAND ----------

# DBTITLE 1,TC_PUSH_045
from azure.identity import DefaultAzureCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import RunFilterParameters
from datetime import datetime, timedelta

test_case = test_cases_dict.get("TC_PUSH_045")
post_test_sql = test_case.get('PostTest_SQL')
test_id = test_case.get('TestID')
test_file_names = test_case.get('Test_File_Names')
landed_folder = test_case.get('Landed_Folder').format(env=env)
test_case_desc = test_case.get('Test_Case_Description') 
error_sql = test_case.get("ErrorSQL")
subscription_id = "80085be5-acec-4711-a455-717ff9d02c08"
resource_group_name = "eip-hsodev-rg"
factory_name = "eip-hsodev-datafactory"

credential = DefaultAzureCredential()
adf_client = DataFactoryManagementClient(credential, subscription_id)
end_time = datetime.now()
start_time = end_time - timedelta(minutes=90)

filter_params = RunFilterParameters(
    last_updated_after=start_time,
    last_updated_before=end_time
)
pipeline_runs = adf_client.pipeline_runs.query_by_factory(
    resource_group_name,
    factory_name,
    filter_params
)
try:
    cnt = 0
    for run in pipeline_runs.value:
        if run.pipeline_name == f"hsodev-{env}-DAP_ADF_Orchestration-etl-pipeline" and run.parameters['FeedName']  == 'centura_daily_autotest':
            cnt+=1
    if cnt > 20:
        display("Testcase TC_PUSH_045 Passed")
        save_output("TC_PUSH_045", test_case_desc,"PASS")
    else:
        display("Testcase TC_PUSH_045 Failed")
        save_output("TC_PUSH_045", test_case_desc,"FAIL",f"{cnt} pipelines triggered in last 60 minutes")
except Exception as e:
    print(f"Testcase {test_id} Failed due to exception: {e}")
    save_output(test_id, test_case_desc, "FAIL", str(e))
    

# COMMAND ----------

# DBTITLE 1,TC_PUSH_046
test_case = test_cases_dict.get("TC_PUSH_046")
if test_case:
    post_test_sql = test_case.get('PostTest_SQL')
    test_id = test_case.get('TestID')
    test_file_names = test_case.get('Test_File_Names')
    test_case_desc = test_case.get('Test_Case_Description')  
    error_sql = test_case.get("ErrorSQL")
    sql_file_path = f'{dbfs_testcase_path}/{post_test_sql}'
    print(test_case_desc)
    print(sql_file_path)
    sql_result = run_post_test_sql(sql_file_path,test_id)
    validator = True
    sources = test_case.get("Sources")
    print("Query execution status of TC_PUSH_046:", sql_result)
    try:
        if sql_result == "PASS":
            targetPaths = test_case.get('Landed_Folder')
            sourcePaths = test_case.get('Source_Path')
            results = {}
            for source in sources:
                results[source] = {}
                parts_count = 3 if source == "claims" else 2
                for i in range(1, parts_count + 1):
                    results[source][f"part{i}"] = {}
                    tgt_directory = targetPaths[source].format(env=env) + str(i)
                    results[source][f"part{i}"]["target_count"] = read_parquet_count_rows(tgt_directory)
                    src_directory = sourcePaths[source].format(env=env , i=i)
                    results[source][f"part{i}"]["source_count"] = read_source_row_count(src_directory)
            print(results)
            for source, parts in results.items():
                for key, value in parts.items():
                    if value["target_count"] != value["source_count"]:
                        validator = False
        if sql_result == "PASS" and validator:
            display("Testcase TC_PUSH_046 Passed")
            save_output("TC_PUSH_046", test_case_desc, "PASS")
        else:
            sql_error_file_path = f'{dbfs_testcase_path}/{error_sql}'
            error=get_error_result_from_sql(sql_error_file_path)
            display("Testcase TC_PUSH_046 Failed")
            save_output("TC_PUSH_046", test_case_desc, "FAIL", error)
    except Exception as e:
        print(f"Testcase {test_id} Failed due to exception: {e}")
        save_output(test_id, test_case_desc, "FAIL", str(e))

# COMMAND ----------

# DBTITLE 1,TC_PUSH_047
test_case = test_cases_dict.get("TC_PUSH_047")
post_test_sql = test_case.get('PostTest_SQL')
test_id = test_case.get('TestID')
test_file_names = test_case.get('Test_File_Names')
landed_folder = test_case.get('Landed_Folder').format(env=env)
test_case_desc = test_case.get('Test_Case_Description')
error_sql = test_case.get("ErrorSQL")

sql_file_path = f'{dbfs_testcase_path}/{post_test_sql}'
print(test_case_desc)
# Call run_sql_file() function to get test result
sql_result = run_post_test_sql(sql_file_path,test_id)
try:
    if sql_result == "PASS":
        display("Testcase TC_PUSH_047 Passed")
        save_output("TC_PUSH_047", test_case_desc,"PASS")
    else:
            sql_error_file_path = f'{dbfs_testcase_path}/{error_sql}'
            error=get_error_result_from_sql(sql_error_file_path)
            display("Testcase TC_PUSH_047 Failed")
            save_output("TC_PUSH_047",test_case_desc, "FAIL",error)
except Exception as e:
    print(f"Testcase {test_id} Failed due to exception: {e}")
    save_output(test_id, test_case_desc, "FAIL", str(e))
    

# COMMAND ----------

# DBTITLE 1,TC_PUSH_048

test_case = test_cases_dict.get("TC_PUSH_048")
if test_case:
    post_test_sql = test_case.get('PostTest_SQL')
    test_id = test_case.get('TestID')
    test_file_names = test_case.get('Test_File_Names')
    test_case_desc = test_case.get('Test_Case_Description')
    error_sql = test_case.get("ErrorSQL")
sql_file_path = f'{dbfs_testcase_path}/{post_test_sql}'
print(test_case_desc)
# Call run_sql_file() function to get test result
sql_result = run_post_test_sql(sql_file_path,test_id)
parts = [2,3]
targetPath = test_case.get('Landed_Folder')
try : 
    for part in parts : 
        tgt_directory = targetPath.format(part=part,env=env)
        read_parquet_count_rows(tgt_directory)
except:
    display("Testcase TC_PUSH_048 Failed")
    save_output("TC_PUSH_048",test_case_desc, "FAIL","the expecting part files are not there in the landed path") 
if sql_result == "PASS":
    display("Testcase TC_PUSH_048 Passed")
    save_output("TC_PUSH_048", test_case_desc,"PASS")
else:
        sql_error_file_path = f'{dbfs_testcase_path}/{error_sql}'
        error=get_error_result_from_sql(sql_error_file_path)
        display("Testcase TC_PUSH_048 Failed")
        save_output("TC_PUSH_048",test_case_desc, "FAIL",error)   
    



# COMMAND ----------

# DBTITLE 1,TC_PUSH_049
from azure.identity import DefaultAzureCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import RunFilterParameters
from datetime import datetime, timedelta
import json

test_case = test_cases_dict.get("TC_PUSH_049")
if test_case:
    test_id = test_case.get('TestID')
    file_names_to_check = test_case.get('Test_File_Names')
    test_case_desc = test_case.get('Test_Case_Description')  
    subscription_id = "80085be5-acec-4711-a455-717ff9d02c08"
    resource_group_name = "eip-hsodev-rg"
    factory_name = "eip-hsodev-datafactory"
    credential = DefaultAzureCredential()
    adf_client = DataFactoryManagementClient(credential, subscription_id)
    end_time = datetime.now()
    start_time = end_time - timedelta(minutes=1200)
    filter_params = RunFilterParameters(
    last_updated_after=start_time,
    last_updated_before=end_time
    )
    pipeline_runs = adf_client.pipeline_runs.query_by_factory(
    resource_group_name,
    factory_name,
    filter_params
    )
    missing_files = []
    try:
        for run in pipeline_runs.value:
            if run.pipeline_name == f"hsodev-{env}-TriggerFAV-etl-pipeline" and run.status == "Succeeded":
                parameters = run.parameters
                fav_filelist = json.loads(parameters['FAVFilelist'])
                file_names_in_run = [file['file_name'] for file in fav_filelist]
                print([file['file_name'] for file in fav_filelist])
                missing_files = [file for file in file_names_to_check if file not in file_names_in_run]
        if not missing_files:
            display("All files are present.")
            save_output("TC_PUSH_049",test_case_desc, "PASS")                
        else:
            display(f"Missing files: {missing_files}")
            save_output("TC_PUSH_049",test_case_desc, "FAIL",f"fav trigger failed for the files : {missing_files}")
    except Exception as e:
        display(f"Error: {e}")
        save_output("TC_PUSH_049",test_case_desc, "FAIL",str(e))

# COMMAND ----------

# DBTITLE 1,TC_PUSH_050
import logging
from azure.monitor.query import LogsQueryClient, LogsQueryStatus
from azure.identity import DefaultAzureCredential
from datetime import timedelta
import re

# Configuration
CONFIG = {
    "app_insights_app_id": "9031317c-052e-4f0d-a7a6-a8c73b3049ba",
    "timespan_hours": 2
}
error = None

def get_ticket_numbers(app_insights_app_id, timespan):
    try:
        credential = DefaultAzureCredential()
        client = LogsQueryClient(credential)

        query = """
        AppTraces
        | where Message contains "ticket"
        | project TimeGenerated, Message
        """

        response = client.query_workspace(
            workspace_id=app_insights_app_id,
            query=query,
            timespan=timespan
        )

        if response.status == LogsQueryStatus.SUCCESS:
            ticket_numbers = []
            for table in response.tables:
                for row in table.rows:
                    message = row[1]  # Access the 'Message' field
                    match = re.search(r"\bUS\d+\b", message)
                    if match:
                        ticket_numbers.append(match.group(0))
            return ticket_numbers
        else:
            error = f"Application Insights query failed: {response.status}"
            return None
    except Exception as e:
        error = f"An unexpected error occurred: {e}"
        return None


if __name__ == "__main__":
    test_case = test_cases_dict.get("TC_PUSH_050")
    test_id = test_case.get('TestID')
    file_names_to_check = test_case.get('Test_File_Names')
    test_case_desc = test_case.get('Test_Case_Description') 
    timespan = timedelta(hours=CONFIG["timespan_hours"])
    ticket_numbers = get_ticket_numbers(CONFIG["app_insights_app_id"], timespan)
    if ticket_numbers:
        print("Ticket Numbers Found:")
        for num in ticket_numbers:
            display(num)
        save_output("TC_PUSH_050",test_case_desc, "PASS")

    else:
        error = "No ticket numbers found or an error occurred."
        display('no rally ticket created')
        save_output("TC_PUSH_050",test_case_desc, "FAIL",error)


# COMMAND ----------

# DBTITLE 1,TC_PUSH_053
test_case = test_cases_dict.get("TC_PUSH_053")
if test_case:
    post_test_sql = test_case.get('PostTest_SQL')
    test_id = test_case.get('TestID')
    test_file_names = test_case.get('Test_File_Names')
    landed_folder = test_case.get('Landed_Folder')
    source_folder = test_case.get('Source_Folder')
    test_case_desc = test_case.get('Test_Case_Description')  
    sql_file_path = f'{dbfs_testcase_path}/{post_test_sql}'
    error_sql = test_case.get("ErrorSQL")
    print(test_case_desc)
    file_path = test_case.get("sorce_folder")

    # Call run_sql_file() function to get test result
    sql_result = run_post_test_sql(sql_file_path, test_id)

    print("Query execution status of TC_PUSH_053:", sql_result)
    if sql_result == "PASS":    
        try:
            source_counts = {}
            i = 1
            for file_name in test_file_names:
                source_counts[i] = read_source_row_count(file_path=file_path.format(dbfs_testcase_path = dbfs_testcase_path , file_name = file_name))
                i += 1
            target_counts = {}
            for part in range(1,3):
                target_counts[part] = read_parquet_count_rows(file_path=landed_folder.format(env = env ,part=part)) 

        except Exception as e:
            print(f"An error occurred: {e}")
        if sql_result == "PASS" and source_counts == target_counts :
            display("Testcase TC_PUSH_053 Passed")
            save_output("TC_PUSH_053", test_case_desc,"PASS")
    else:
        sql_error_file_path = f'{dbfs_testcase_path}/{error_sql}'
        res=get_error_result_from_sql(sql_error_file_path)
        display("Testcase TC_PUSH_053 Failed")
        save_output("TC_PUSH_053", test_case_desc,"FAIL", error)

# COMMAND ----------

# DBTITLE 1,Testcase Report
import csv  
from datetime import datetime  
  
# Define the current timestamp  
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')  
  
# Define the path to save the CSV report with timestamp suffix  
csv_report_path = f'/dbfs/mnt/data/{env}/Test_files/regression/test_case_report_{timestamp}.csv'  

with open(csv_report_path, mode='w', newline='') as file:  
    writer = csv.writer(file)  
    writer.writerow(['Test Case',  'Status', 'Description' , 'Error Message'])  
    for test_case, status in outputs.items():  
        writer.writerow([test_case, status['result'], status['description'], status['error']])  
  
# Display the CSV file path  
df = spark.read.csv(f'dbfs:/mnt/data/{env}/Test_files/regression/test_case_report_{timestamp}.csv', header=True) 
ordered_df = df.orderBy("Test Case") 
display(ordered_df)

# COMMAND ----------

total_tests = df.count()
passed_tests = df.filter(df["Status"] == 'PASS').count()
failed_tests = df.filter(df["Status"] == 'FAIL').count()
pass_percentage = (passed_tests / total_tests) * 100

# COMMAND ----------

print(pass_percentage)

# COMMAND ----------

import pandas as pd
from datetime import datetime, timezone, timedelta
import requests



def create_email_body_from_csv_pandas(ordered_df):
    """Creates an email body with a CSV file as a formatted table using pandas."""
    try:
        df = ordered_df.toPandas()
        html_table = df.to_html(index=False, justify='center', border=1) #index=False removes row index,justify center aligns cells
        email_body = f"""<h2>Please find the below test case report :</h2>\n <p>Total Test Cases: {total_tests}</p>\n<p>Passed Test Cases: {passed_tests}</p>\n<p>Failed Test Cases: {failed_tests}</p>\n<p>Pass Percentage: {pass_percentage:.2f}%</p>\n\n{html_table}""" # Add HTML header
        return email_body
    except FileNotFoundError:
        print(f"Error: CSV file not found at {ordered_df}")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

body = create_email_body_from_csv_pandas(ordered_df)

dap_nonprod_emailer_URL = get_secret(dap_nonprod_emailer_URL_nm, key_vault_name)
today = datetime.now().astimezone().astimezone(timezone(timedelta(hours=5, minutes=30))).strftime("%Y-%m-%d %H:%M")
subject = f"DAP regression automation {env.upper()} Test Case Report - {today}"
recipients = "alavala_babu@optum.com;ss@optum.com"#jsonObject.get('Recipients')
sender = "odedap@omail.o360.cloud"
json_data = {"message": body,"subject": subject, "receiver":recipients, "from": sender} 

output = requests.post(dap_nonprod_emailer_URL, json=json_data)
print(output.status_code)

