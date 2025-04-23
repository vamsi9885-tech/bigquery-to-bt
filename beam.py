

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions , 
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
    table_id = "gear-cars-logs-apachebeam"
    column_family = "cf1"
    row_key_column = "Request_ID"
    options = PipelineOptions(
        project = project_id,
        temp_location = 'gs://prj-d-gbl-gar-epgear/gear_hist_finapi/sample_testing/testing_akbehe/temp',
        region='us-central1',
        runner='SparkRunner',
        job_name='bq-to-bt-parquet'
        )
    logging.info("started the beam operation")
    with beam.Pipeline(options=options) as p:
        (
            p
            | "Read from Parquet" >> beam.io.ReadFromParquet(gcs_path)
            | "Convert to Bigtable Row" >> beam.ParDo(ConvertToBigtableRow(row_key_column, column_family))
            | "Write to Bigtable" >> WriteToBigTable(bt_project_id, instance_id, table_id)
        )
if __name__ == "__main__":
    run()
