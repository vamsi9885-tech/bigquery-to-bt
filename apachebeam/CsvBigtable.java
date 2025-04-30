package com.aexp.ngbd.testDataflow;

// Importing required Libraries
import com.google.cloud.bigtable.beam.CloudBigtableIO;
import com.google.cloud.bigtable.beam.CloudBigtableScanConfiguration;
import com.google.cloud.bigtable.beam.CloudBigtableTableConfiguration;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.hadoop.hbase.client.Result;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Date;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Default;

import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubOptions;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;

import org.joda.time.Duration;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.Create;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.nio.charset.StandardCharsets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.json.JSONObject;
import com.opencsv.CSVReader;
import java.io.StringReader;


// Defining Class
public class CsvBigtable {

//	Defining Pipeline options
	public interface BigtableOptions extends PipelineOptions {
	    @Description("The Bigtable project ID, this can be different than your Dataflow project")
	    @Default.String("lumi-local")
	    String getBigtableProjectId();
	
	    void setBigtableProjectId(String bigtableProjectId);
	
	    @Description("The Bigtable instance ID")
	    @Default.String("fake_instance")
	    String getBigtableInstanceId();
	
	    void setBigtableInstanceId(String bigtableInstanceId);
	
	    @Description("The Bigtable table ID in the instance.")
	    @Default.String("test_table")
	    String getBigtableTableId();
	
	    void setBigtableTableId(String bigtableTableId);
	    
	    @Description("The Bigtable table Column Family.")
	    @Default.String("cf1")
	    String getBigtableColumnFamily();
	
	    void setBigtableColumnFamily(String bigtableColumnFamily);
	    
	    @Description("The Bigtable table Row Key.")
	    @Default.String("phone#4c410523#20190501")
	    String getBigtableRowKey();
	
	    void setBigtableRowKey(String bigtableRowKey);
	    
	    @Description("Input csv file")
	    @Default.String("storage")
	    String getInput();

	    void setInput(String value);
	    
	    @Description("The Bigtable Columns list.")
	    @Default.String("connected_cell,connected_wifi,os_build")
	    String getBigtableColumns();
	
	    void setBigtableColumns(String bigtableColumns);

		@Description("The Bigtable Columns list.")
		@Default.String("connected_cell,connected_wifi,os_build")
		String getJsonContent();

		void setJsonContent(String jsonContent);
	  }


//	  Main Class
	public static void main(String[] args) {
		
	    BigtableOptions options =
	        PipelineOptionsFactory.fromArgs(args).withValidation().as(BigtableOptions.class);	    
	    
	    Pipeline pipeline  = Pipeline.create(options);	  
	
	    String ProjectId = options.getBigtableProjectId();
	    String InstanceId = options.getBigtableInstanceId();
	    String TableId = options.getBigtableTableId();
	    String ColumnFamily = options.getBigtableColumnFamily();
	    String RowKey = options.getBigtableRowKey();
	    String InputFile = options.getInput();
	    String TableColumns = options.getBigtableColumns();
		String jsonContent = options.getJsonContent();

	    
	    //Print input values for testing
	    /*
	    System.out.println(ProjectId);
	    System.out.println(InstanceId);
	    System.out.println(TableId);
	    System.out.println(ColumnFamily);
	    System.out.println(RowKey);
	    System.out.println(InputFile);
	    System.out.println(TableColumns);
	    */
	    	    
	    CloudBigtableTableConfiguration config =
	            new CloudBigtableTableConfiguration.Builder()
	                .withProjectId(options.getBigtableProjectId())
	                .withInstanceId(options.getBigtableInstanceId())
	                .withTableId(options.getBigtableTableId())
	                .build();
	    
	    System.out.println("Pipeline Execution Started");
	    
	    pipeline
	    .apply("DefineCsvFileMatches", FileIO.match().filepattern(InputFile))
        .apply("MatchCsvFiles", FileIO.readMatches())
        .apply("ReadCsvFiles", TextIO.readFiles())
	    .apply(
		        ParDo.of(
		            new DoFn<String, Mutation>() {
		              @ProcessElement
		              public void processElement(ProcessContext ctx, OutputReceiver<Mutation> out) {
		      	        
		            	//System.out.println("Input Row: " + ctx.element());	  
		      	        String input = ctx.element();
		      	        
//		      	        String[] input_rows = input.split(",");
						String[] input_rows = null;
						try{
							CSVReader reader = new CSVReader(new StringReader(input));
							input_rows = reader.readNext();
							reader.close();
						}
						catch (Exception e){
							throw new RuntimeException("Error parsing CSV line :" + input , e);
						}
		      	        String[] input_columns = TableColumns.split(",");
		      	        		      	       
		      	        long timestamp = System.currentTimeMillis();
						  LocalDate currentDate = LocalDate.now();
						  DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
						  String formattedDate = currentDate.format(formatter);
						int rowKeyIndex = -1;
						for (int i =0 ; i < input_columns.length ; i++){
							if (input_columns[i].trim().equals(RowKey)){
								rowKeyIndex = i;
								break;
							}
						}
						String rowKeyDynamic = input_rows[rowKeyIndex] + "_" + formattedDate;
		      	        Put row = new Put(Bytes.toBytes(rowKeyDynamic));


						  for (int i = 0; i<input_rows.length; i++) {

							  //System.out.println("Input Row value: " + input_rows[i]);

							  for(int j = 0; j<input_columns.length; j++){

								  if (i==j)
								  {
									  //System.out.println("Table Column Name: " + input_columns[j]);
									  row.addColumn(
											  Bytes.toBytes(ColumnFamily),
											  Bytes.toBytes(input_columns[j]),
											  timestamp,
											  Bytes.toBytes(input_rows[i]));
								  }
							  }
						  }
		                out.output(row);
		      	      }
		            }))
	    
	    .apply(CloudBigtableIO.writeToTable(config));
	    
	    System.out.println("Pipeline steps are executed");

//		Running Pipeline to load data from CSV to Bigtable
	    pipeline.run();
	    
	    System.out.println("Pipeline execution completed");
        	   
	  }


}