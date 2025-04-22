Traceback (most recent call last):
  File "/tmp/lumiadmingearbqtobt-1745335447-a06a6665/beam.py", line 5, in <module>
    from apache_beam.io.gcp.bigtableio import WriteToBigTable
  File "/opt/conda/default/lib/python3.10/site-packages/apache_beam/io/gcp/bigtableio.py", line 60, in <module>
    from google.cloud.bigtable import Client
  File "/opt/conda/default/lib/python3.10/site-packages/google/cloud/bigtable/__init__.py", line 17, in <module>
    from google.cloud.bigtable.client import Client
  File "/opt/conda/default/lib/python3.10/site-packages/google/cloud/bigtable/client.py", line 38, in <module>
    from google.cloud import bigtable_admin_v2
  File "/opt/conda/default/lib/python3.10/site-packages/google/cloud/bigtable_admin_v2/__init__.py", line 21, in <module>
    from .services.bigtable_instance_admin import BigtableInstanceAdminClient
  File "/opt/conda/default/lib/python3.10/site-packages/google/cloud/bigtable_admin_v2/services/bigtable_instance_admin/__init__.py", line 16, in <module>
    from .client import BigtableInstanceAdminClient
  File "/opt/conda/default/lib/python3.10/site-packages/google/cloud/bigtable_admin_v2/services/bigtable_instance_admin/client.py", line 74, in <module>
    from .transports.base import BigtableInstanceAdminTransport, DEFAULT_CLIENT_INFO
  File "/opt/conda/default/lib/python3.10/site-packages/google/cloud/bigtable_admin_v2/services/bigtable_instance_admin/transports/__init__.py", line 19, in <module>
    from .base import BigtableInstanceAdminTransport
  File "/opt/conda/default/lib/python3.10/site-packages/google/cloud/bigtable_admin_v2/services/bigtable_instance_admin/transports/base.py", line 26, in <module>
    from google.api_core import operations_v1
  File "/opt/conda/default/lib/python3.10/site-packages/google/api_core/operations_v1/__init__.py", line 17, in <module>
    from google.api_core.operations_v1.abstract_operations_client import AbstractOperationsClient
  File "/opt/conda/default/lib/python3.10/site-packages/google/api_core/operations_v1/abstract_operations_client.py", line 22, in <module>
    from google.api_core.operations_v1.transports.base import (
  File "/opt/conda/default/lib/python3.10/site-packages/google/api_core/operations_v1/transports/__init__.py", line 29, in <module>
    from .rest_asyncio import AsyncOperationsRestTransport
  File "/opt/conda/default/lib/python3.10/site-packages/google/api_core/operations_v1/transports/rest_asyncio.py", line 23, in <module>
    from google.auth.aio.transport.sessions import AsyncAuthorizedSession  # type: ignore
  File "/opt/conda/default/lib/python3.10/site-packages/google/auth/aio/transport/sessions.py", line 27, in <module>
    from google.auth.aio.transport.aiohttp import Request as AiohttpRequest
  File "/opt/conda/default/lib/python3.10/site-packages/google/auth/aio/transport/aiohttp.py", line 22, in <module>
    import aiohttp  # type: ignore
  File "/opt/conda/default/lib/python3.10/site-packages/aiohttp/__init__.py", line 6, in <module>
    from .client import (
  File "/opt/conda/default/lib/python3.10/site-packages/aiohttp/client.py", line 35, in <module>
    from . import hdrs, http, payload
  File "/opt/conda/default/lib/python3.10/site-packages/aiohttp/http.py", line 7, in <module>
    from .http_parser import (
  File "/opt/conda/default/lib/python3.10/site-packages/aiohttp/http_parser.py", line 15, in <module>
    from .helpers import NO_EXTENSIONS, BaseTimerContext
  File "/opt/conda/default/lib/python3.10/site-packages/aiohttp/helpers.py", line 667, in <module>
    class CeilTimeout(async_timeout.timeout):
TypeError: function() argument 'code' must be code, not str




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
