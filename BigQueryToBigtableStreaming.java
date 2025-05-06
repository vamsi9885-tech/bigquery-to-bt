
package com.example.beam;

import com.google.bigtable.v2.Mutation;
import com.google.protobuf.ByteString;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.io.gcp.bigquery.TableRow;
import com.google.cloud.bigtable.config.BigtableOptions;

import java.util.Map;

public class BigQueryToBigtableStreaming {

    public interface Options extends org.apache.beam.sdk.options.PipelineOptions {
        @Validation.Required
        String getBigtableProjectId();
        void setBigtableProjectId(String value);

        @Validation.Required
        String getBigtableInstanceId();
        void setBigtableInstanceId(String value);

        @Validation.Required
        String getBigtableTableId();
        void setBigtableTableId(String value);
    }

    public static void main(String[] args) {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        Pipeline p = Pipeline.create(options);

        p.apply("ReadFromBigQuery", BigQueryIO.readTableRows()
                .fromQuery("SELECT * FROM `project.dataset.table` WHERE partition_date = '2024-04-01'")
                .usingStandardSql()
                .withMethod(BigQueryIO.TypedRead.Method.DIRECT_READ))
         .apply("ConvertToMutations", ParDo.of(new DoFn<TableRow, KV<ByteString, Mutation>>() {
             @ProcessElement
             public void processElement(ProcessContext c) {
                 TableRow row = c.element();
                 String rowKey = row.get("rowkey").toString(); // ensure this field exists
                 for (Map.Entry<String, Object> entry : row.entrySet()) {
                     if ("rowkey".equals(entry.getKey())) continue;

                     Mutation.SetCell cell = Mutation.SetCell.newBuilder()
                             .setFamilyName("cf1")
                             .setColumnQualifier(ByteString.copyFromUtf8(entry.getKey()))
                             .setValue(ByteString.copyFromUtf8(String.valueOf(entry.getValue())))
                             .build();

                     Mutation mutation = Mutation.newBuilder().setSetCell(cell).build();
                     c.output(KV.of(ByteString.copyFromUtf8(rowKey), mutation));
                 }
             }
         }))
         .apply("WriteToBigtable", BigtableIO.write()
                 .withProjectId(options.getBigtableProjectId())
                 .withInstanceId(options.getBigtableInstanceId())
                 .withTableId(options.getBigtableTableId())
         );

        p.run();
    }
}
