
from azure.identity import DefaultAzureCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import RunFilterParameters
from datetime import datetime, timedelta

SUBSCRIPTION_ID = "80085be5-acec-4711-a455-717ff9d02c08"
RESOURCE_GROUP_NAME = "eip-hsodev-rg"
FACTORY_NAME = "eip-hsodev-datafactory"

def get_pipeline_runs(pipeline_name, minutes_delta=60, env="qa"):
    credential = DefaultAzureCredential()
    adf_client = DataFactoryManagementClient(credential, SUBSCRIPTION_ID)
    end_time = datetime.now()
    start_time = end_time - timedelta(minutes=minutes_delta)

    filter_params = RunFilterParameters(
        last_updated_after=start_time,
        last_updated_before=end_time
    )

    pipeline_runs = adf_client.pipeline_runs.query_by_factory(
        RESOURCE_GROUP_NAME,
        FACTORY_NAME,
        filter_params
    )

    matched_runs = [
        run for run in pipeline_runs.value
        if run.pipeline_name == pipeline_name or run.pipeline_name == f"hsodev-{env}-{pipeline_name}"
    ]
    return matched_runs
