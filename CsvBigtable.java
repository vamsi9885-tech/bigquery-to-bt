"select distinct on (logical_file_name,part) FAS.logical_file_name, FAS.part, FAS.extraction_type, FAS.file_name_format, FAS.cadence_id from file_arrival_status as FAS where FAS.feed_name = '{$FeedName}' and not exists (select 1 from file_master AS FM  where FM.logical_file_name = FAS.logical_file_name AND FM.part = FAS.part AND FM.cadence_id = '{$CadenceID}')" 


spring.application.name=api
application.environment=local
application.ui.port=3000
server.servlet.session.timeout=30m
DISABLE_AUTH=false
springdoc.swagger-ui.path=/api/portal-api/swagger-ui
#springdoc.api-docs.path=/portal-api/v3/api-docs
HomePageURL=http://localhost:3000
springdoc.api-docs.path=/api/v3/api-docs
springdoc.api-docs.version=openapi_3_1
spring.profiles.active=dev
spring.flyway.enabled=true
spring.flyway.baseline-on-migrate=true
spring.flyway.baseline-version=0
# spring.datasource.url=jdbc:postgresql://localhost:5432/Dap
logging.level.org.springframework.security=DEBUG
# spring.datasource.url=jdbc:postgresql://dap-nonprod-postgresql-pe.postgres.database.azure.com:5432/dev_dap_run_log
# spring.datasource.username=eip_dap_admin
spring.datasource.url=jdbc:postgresql://localhost:5432/daptest
spring.datasource.username=postgres
spring.datasource.password=postgres
spring.jpa.show-sql=true
spring.datasource.driver-class-name=org.postgresql.Driver
spring.jpa.properties.hibernate.dialect=org.hibernate.dialect.PostgreSQLDialect
spring.session.store-type=jdbc
# spring.jpa.hibernate.ddl-auto=none
oidc.clientId=dap-dev
oidc.clientSecret=bPlD7s37GlHVM12lE8z0qdudg8VfHQKK
spring.jpa.properties.hibernate.temp.use_jdbc_metadata_defaults=false
spring.jpa.hibernate.ddl-auto=none
logging.pattern.console=%d{yyyy-MM-dd HH:mm:ss} - %logger{36} -[%X{correlationId}] -[%X{loggedInUsername}]- %msg%n
server.servlet.session.cookie.secure=true
server.servlet.session.cookie.http-only=true
server.servlet.session.cookie.same-site=None
oidc.issuerUri=https://idx-stage.linkhealth.com/auth/realms/developer-platform
oidc.logoutEndpoint=https://idx-stage.linkhealth.com/auth/realms/developer-platform/protocol/openid-connect/logout
oidc.userInfoEndpoint=https://idx-stage.linkhealth.com/auth/realms/developer-platform/protocol/openid-connect/userinfo
oidc.tokenEndpoint=https://idx-stage.linkhealth.com/auth/realms/developer-platform/protocol/openid-connect/token
oidc.authorizationEndpoint=https://idx-stage.linkhealth.com/auth/realms/developer-platform/protocol/openid-connect/auth
oidc.jwkSetUri=https://idx-stage.linkhealth.com/auth/realms/developer-platform/protocol/openid-connect/certs
m2moidc.clientWriteClaimName=MR_WriteClaim
m2moidc.clientWriteClaimValue=c1ee643c-6bd6-463d-a867-37b63401aed0
spring.mvc.throw-exception-if-no-handler-found=true
spring.web.resources.add-mappings=false
auth.required.ad.group=AZU_GHEC_OI_ANALYTICS_DAP_DATA_ACQUISITION_PLATFORM_WRITE
auth.skip.authorization=false
logging.pattern.level=%5p [${spring.application.name:},%X{correlationId}]
logging.level.org.springframework.security.web.FilterChainProxy=INFO
logging.level.org.springframework.security.web.context.HttpSessionSecurityContextRepository=OFF





mvn archetype:generate -DgroupId=com.aexp.ngbd.testDataflow -DartifactId=CsvBigtableApp -DarchetypeArtifactId=maven-archetype-quickstart -DinteractiveMode=false


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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.json.JSONObject;

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
		      	        
		      	        String[] input_rows = input.split(",");
		      	        String[] input_columns = TableColumns.split(",");
		      	        		      	       
		      	        long timestamp = System.currentTimeMillis();
		      	        Put row = new Put(Bytes.toBytes(RowKey));
		      	        
		      	        
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
