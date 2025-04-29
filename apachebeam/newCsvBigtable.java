package com.aexp.ngbd.testDataflow;

// Importing required Libraries
import com.google.cloud.bigtable.beam.CloudBigtableIO;
import com.google.cloud.bigtable.beam.CloudBigtableTableConfiguration;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.StringReader;
import java.util.List;

public class newCsvBigtable {

    public interface BigtableOptions extends PipelineOptions {
        @Description("The Bigtable project ID")
        @Default.String("lumi-local")
        String getBigtableProjectId();
        void setBigtableProjectId(String bigtableProjectId);

        @Description("The Bigtable instance ID")
        @Default.String("fake_instance")
        String getBigtableInstanceId();
        void setBigtableInstanceId(String bigtableInstanceId);

        @Description("The Bigtable table ID")
        @Default.String("test_table")
        String getBigtableTableId();
        void setBigtableTableId(String bigtableTableId);

        @Description("The Bigtable Column Family")
        @Default.String("cf1")
        String getBigtableColumnFamily();
        void setBigtableColumnFamily(String bigtableColumnFamily);

        @Description("Input CSV file")
        @Default.String("storage")
        String getInput();
        void setInput(String value);

        @Description("The Bigtable Columns list")
        @Default.String("connected_cell,connected_wifi,os_build")
        String getBigtableColumns();
        void setBigtableColumns(String bigtableColumns);
    }

    public static void main(String[] args) {

        BigtableOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(BigtableOptions.class);
        Pipeline pipeline = Pipeline.create(options);

        CloudBigtableTableConfiguration config = new CloudBigtableTableConfiguration.Builder()
                .withProjectId(options.getBigtableProjectId())
                .withInstanceId(options.getBigtableInstanceId())
                .withTableId(options.getBigtableTableId())
                .build();

        String inputFile = options.getInput();
        String columnFamily = options.getBigtableColumnFamily();
        String[] tableColumns = options.getBigtableColumns().split(",");

        System.out.println("Pipeline Execution Started");

        pipeline
                .apply("MatchCsvFiles", FileIO.match().filepattern(inputFile))
                .apply("ReadCsvFiles", FileIO.readMatches())
                .apply("ExtractLines", TextIO.readFiles())
                .apply("ProcessCsvLines", ParDo.of(new DoFn<String, Mutation>() {
                    @ProcessElement
                    public void processElement(ProcessContext ctx, OutputReceiver<Mutation> out) throws Exception {
                        String input = ctx.element();

                        // Proper CSV parsing
                        CSVParser parser = CSVFormat.DEFAULT
                                .withIgnoreSurroundingSpaces()
                                .withTrim()
                                .withQuote('"')
                                .parse(new StringReader(input));

                        List<CSVRecord> records = parser.getRecords();
                        if (records.isEmpty()) {
                            return; // Skip empty line
                        }

                        CSVRecord record = records.get(0);

                        // Generate dynamic Row Key (example: combine first two columns)
                        String rowKeyDynamic = record.get(0) + "#" + record.get(1); // Customize as needed
                        Put row = new Put(Bytes.toBytes(rowKeyDynamic));

                        long timestamp = System.currentTimeMillis();

                        // Mapping columns
                        for (int i = 0; i < tableColumns.length && i < record.size(); i++) {
                            String columnName = tableColumns[i];
                            String value = record.get(i);

                            row.addColumn(
                                    Bytes.toBytes(columnFamily),
                                    Bytes.toBytes(columnName),
                                    timestamp,
                                    Bytes.toBytes(value)
                            );
                        }

                        out.output(row);
                    }
                }))
                .apply(CloudBigtableIO.writeToTable(config));

        pipeline.run();
        System.out.println("Pipeline Execution Completed");
    }
}
