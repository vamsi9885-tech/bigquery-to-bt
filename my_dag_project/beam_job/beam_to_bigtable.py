
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import sys

class WriteToBigtable(beam.DoFn):
    def __init__(self, project_id, instance_id, table_id):
        from google.cloud import bigtable
        self.client = bigtable.Client(project=project_id, admin=True)
        self.table = self.client.instance(instance_id).table(table_id)

    def process(self, element):
        from google.cloud.bigtable.row import DirectRow
        row = DirectRow(row_key=str(element['id']).encode())
        row.set_cell('cf1', 'name', element['name'])
        yield self.table.mutate_rows([row])

def run():
    options = PipelineOptions()
    input_path = sys.argv[1]

    with beam.Pipeline(options=options) as p:
        (
            p
            | "ReadParquet" >> beam.io.ReadFromParquet(input_path)
            | "ToBigtable" >> beam.ParDo(WriteToBigtable(
                project_id='your-gcp-project-id',
                instance_id='your-bigtable-instance',
                table_id='your-table-name'
            ))
        )

if __name__ == '__main__':
    run()
