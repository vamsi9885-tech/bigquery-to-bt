WARNING:apache_beam.options.pipeline_options:Unknown pipeline options received: --gear_project_id=prj-d-gbl-gar-epgear,--bt_project_name=prj-d-lumi-bt,--instance_id=lumiplfeng-decppgusbt241030202635,--bt_table_name=gear_cars_test1,--bq_table_name=cars_logs,--dataset_id=temp,--json_file=gear_hist_finapi/sample_testing/bq2bt_otl_mapping/cars_log.json,--row_key=Request_ID. Ignore if flags are used for internal purposes.
WARNING:apache_beam.options.pipeline_options:Unknown pipeline options received: --gear_project_id=prj-d-gbl-gar-epgear,--bt_project_name=prj-d-lumi-bt,--instance_id=lumiplfeng-decppgusbt241030202635,--bt_table_name=gear_cars_test1,--bq_table_name=cars_logs,--dataset_id=temp,--json_file=gear_hist_finapi/sample_testing/bq2bt_otl_mapping/cars_log.json,--row_key=Request_ID. Ignore if flags are used for internal purposes.
Traceback (most recent call last):
  File "/tmp/lumiadmingearbqtobt-1745381060-f67be432/beam.py", line 47, in <module>
    run()
  File "/tmp/lumiadmingearbqtobt-1745381060-f67be432/beam.py", line 39, in run
    with beam.Pipeline(options=options) as p:
  File "/opt/conda/default/lib/python3.10/site-packages/apache_beam/pipeline.py", line 612, in __exit__
    self.result = self.run()
  File "/opt/conda/default/lib/python3.10/site-packages/apache_beam/pipeline.py", line 562, in run
    self._options).run(False)
  File "/opt/conda/default/lib/python3.10/site-packages/apache_beam/pipeline.py", line 586, in run
    return self.runner.run_pipeline(self, self._options)
  File "/opt/conda/default/lib/python3.10/site-packages/apache_beam/runners/dataflow/dataflow_runner.py", line 485, in run_pipeline
    self.dataflow_client.create_job(self.job), self)
  File "/opt/conda/default/lib/python3.10/site-packages/apache_beam/utils/retry.py", line 298, in wrapper
    return fun(*args, **kwargs)
  File "/opt/conda/default/lib/python3.10/site-packages/apache_beam/runners/dataflow/internal/apiclient.py", line 735, in create_job
    return self.submit_job_description(job)
  File "/opt/conda/default/lib/python3.10/site-packages/apache_beam/utils/retry.py", line 298, in wrapper
    return fun(*args, **kwargs)
  File "/opt/conda/default/lib/python3.10/site-packages/apache_beam/runners/dataflow/internal/apiclient.py", line 835, in submit_job_description
    response = self._client.projects_locations_jobs.Create(request)
  File "/opt/conda/default/lib/python3.10/site-packages/apache_beam/runners/dataflow/internal/clients/dataflow/dataflow_v1b3_client.py", line 722, in Create
    return self._RunMethod(config, request, global_params=global_params)
  File "/opt/conda/default/lib/python3.10/site-packages/apitools/base/py/base_api.py", line 731, in _RunMethod
    return self.ProcessHttpResponse(method_config, http_response, request)
  File "/opt/conda/default/lib/python3.10/site-packages/apitools/base/py/base_api.py", line 737, in ProcessHttpResponse
    self.__ProcessHttpResponse(method_config, http_response, request))
  File "/opt/conda/default/lib/python3.10/site-packages/apitools/base/py/base_api.py", line 603, in __ProcessHttpResponse
    raise exceptions.HttpError.FromResponse(
apitools.base.py.exceptions.HttpBadRequestError: HttpError accessing <https://dataflow.googleapis.com/v1b3/projects/prj-d-gbl-gar-epgear/locations/US/jobs?alt=json>: response: <{'vary': 'Origin, X-Origin, Referer', 'content-type': 'application/json; charset=UTF-8', 'date': 'Wed, 23 Apr 2025 04:04:39 GMT', 'server': 'ESF', 'x-xss-protection': '0', 'x-frame-options': 'SAMEORIGIN', 'x-content-type-options': 'nosniff', 'transfer-encoding': 'chunked', 'status': '400', 'content-length': '503', '-content-encoding': 'gzip'}>, content <{
  "error": {
    "code": 400,
    "message": "(fada61b897668351): The workflow could not be created, since it was sent to an invalid regional endpoint (US) or the wrong API endpoint was used. Please resubmit to a valid Cloud Dataflow regional endpoint and ensure you are using dataflow.projects.locations.templates.launch API endpoint. The list of Cloud Dataflow regional endpoints is at https://cloud.google.com/dataflow/docs/concepts/regional-endpoints. ",
    "status": "FAILED_PRECONDITION"
  }
}
>




import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigtableio import WriteToBigTable
from google.cloud.bigtable.row import DirectRow
import pyarrow.parquet as pq
import logging
import os

class ConvertToBigtableRow(beam.DoFn):
    def __init__(self, row_key_field, column_family):
        logging.info("started converting to bigtable")
        self.row_key_field = row_key_field
        self.column_family = column_family
    def process(self, element):
        row_key = str(element[self.row_key_field]).encode("utf-8")
        row = DirectRow(row_key=row_key)
        for col, val in element.items():
            if col != self.row_key_field and val is not None:
                row.set_cell(self.column_family, col, str(val).encode("utf-8"))
        yield row
def run():
    gcs_path = 'gs://prj-d-gbl-gar-epgear/gear_hist_finapi/sample_testing/testing_akbehe/cars_logs_*.parquet'
    project_id = "prj-d-gbl-gar-epgear"
    bt_project_id = "prj-d-lumi-bt"
    instance_id = "lumiplfeng-decppgusbt241030202635"
    table_id = "cars-logs-apachebeam"
    column_family = "cf1"
    row_key_column = "Request_ID"
    options = PipelineOptions(
        project = project_id,
        temp_location = 'gs://prj-d-gbl-gar-epgear/gear_hist_finapi/sample_testing/testing_akbehe/temp',
        region='US',
        runner='DataflowRunner',
        job_name='bq-to-bt-parquet'
        )
    logging("started the beam operation")
    with beam.Pipeline(options=options) as p:
        (
            p
            | "Read from Parquet" >> beam.io.ReadFromParquet(gcs_path)
            | "Convert to Bigtable Row" >> beam.ParDo(ConvertToBigtableRow(row_key_column, column_family))
            | "Write to Bigtable" >> WriteToBigTable(bt_project_id, instance_id, table_id)
        )
if __name__ == "__main__":
    run()
