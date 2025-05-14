25/05/14 11:38:58 INFO SparkEnv: Registering MapOutputTracker
25/05/14 11:38:59 INFO SparkEnv: Registering BlockManagerMaster
25/05/14 11:38:59 INFO SparkEnv: Registering BlockManagerMasterHeartbeat
25/05/14 11:38:59 INFO SparkEnv: Registering OutputCommitCoordinator
25/05/14 11:39:01 INFO DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at gear-bal-cluster-1-m.us-central1-f.c.prj-d-gbl-gar-epgear.internal./172.28.128.6:8032
25/05/14 11:39:01 INFO AHSProxy: Connecting to Application History server at gear-bal-cluster-1-m.us-central1-f.c.prj-d-gbl-gar-epgear.internal./172.28.128.6:10200
25/05/14 11:39:03 INFO Configuration: resource-types.xml not found
25/05/14 11:39:03 INFO ResourceUtils: Unable to find 'resource-types.xml'.
25/05/14 11:39:05 INFO YarnClientImpl: Submitted application application_1747222507939_0001
25/05/14 11:39:06 INFO DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at gear-bal-cluster-1-m.us-central1-f.c.prj-d-gbl-gar-epgear.internal./172.28.128.6:8030
25/05/14 11:39:12 INFO GoogleCloudStorageImpl: Ignoring exception of type GoogleJsonResponseException; verified object already exists with desired state.
25/05/14 11:39:13 INFO GoogleHadoopSyncableOutputStream: hflush(): No-op due to rate limit (RateLimiter[stableRate=0.3qps]): readers will *not* yet see flushed data for gs://prj-d-lumicommon-phs/spark-history/application_1747222507939_0001.inprogress [CONTEXT ratelimit_period="1 MINUTES" ]
table alrady exists
25/05/14 11:39:35 INFO DirectBigQueryRelation: |Querying table prj-d-gbl-gar-epgear.temp._bqc_d5b4ac992b44422fa195e18fc6a46dd3, parameters sent from Spark:|requiredColumns=[bq_rowkey,f0_],|filters=[]
25/05/14 11:39:39 INFO ReadSessionCreator: Requested 20000 max partitions, but only received 1 from the BigQuery Storage API for session projects/prj-d-gbl-gar-epgear/locations/us/sessions/CAISDHdTWUZlYlpJQzM2MRoCb3YaAm93. Notice that the number of streams in actual may be lower than the requested number, depending on the amount parallelism that is reasonable for the table and the maximum amount of parallelism allowed by the system.
25/05/14 11:39:39 INFO BigQueryRDDFactory: Created read session for table 'prj-d-gbl-gar-epgear.temp._bqc_d5b4ac992b44422fa195e18fc6a46dd3': projects/prj-d-gbl-gar-epgear/locations/us/sessions/CAISDHdTWUZlYlpJQzM2MRoCb3YaAm93
Exception in thread "main" java.lang.ClassNotFoundException: 
Failed to find data source: bigtable. Please find packages at
https://spark.apache.org/third-party-projects.html
       
	at org.apache.spark.sql.errors.QueryExecutionErrors$.failedToFindDataSourceError(QueryExecutionErrors.scala:573)
	at org.apache.spark.sql.execution.datasources.DataSource$.lookupDataSource(DataSource.scala:675)
	at org.apache.spark.sql.execution.datasources.DataSource$.lookupDataSourceV2(DataSource.scala:725)
	at org.apache.spark.sql.DataFrameWriter.lookupV2Provider(DataFrameWriter.scala:864)
	at org.apache.spark.sql.DataFrameWriter.saveInternal(DataFrameWriter.scala:256)
	at org.apache.spark.sql.DataFrameWriter.save(DataFrameWriter.scala:247)
	at com.example.bqtobt.SparkBQToBT.exportDataToBigtable(SparkBQToBT.java:124)
	at com.example.bqtobt.SparkBQToBT.main(SparkBQToBT.java:40)
	at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
	at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
	at java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
	at java.base/java.lang.reflect.Method.invoke(Method.java:566)
	at org.apache.spark.deploy.JavaMainApplication.start(SparkApplication.scala:52)
	at org.apache.spark.deploy.SparkSubmit.org$apache$spark$deploy$SparkSubmit$$runMain(SparkSubmit.scala:958)
	at org.apache.spark.deploy.SparkSubmit.doRunMain$1(SparkSubmit.scala:180)
	at org.apache.spark.deploy.SparkSubmit.submit(SparkSubmit.scala:203)
	at org.apache.spark.deploy.SparkSubmit.doSubmit(SparkSubmit.scala:90)
	at org.apache.spark.deploy.SparkSubmit$$anon$2.doSubmit(SparkSubmit.scala:1046)
	at org.apache.spark.deploy.SparkSubmit$.main(SparkSubmit.scala:1055)
	at org.apache.spark.deploy.SparkSubmit.main(SparkSubmit.scala)
Caused by: java.lang.ClassNotFoundException: bigtable.DefaultSource
	at java.base/java.net.URLClassLoader.findClass(URLClassLoader.java:476)
	at java.base/java.lang.ClassLoader.loadClass(ClassLoader.java:589)
	at java.base/java.lang.ClassLoader.loadClass(ClassLoader.java:522)
	at org.apache.spark.sql.execution.datasources.DataSource$.$anonfun$lookupDataSource$5(DataSource.scala:661)
	at scala.util.Try$.apply(Try.scala:213)
	at org.apache.spark.sql.execution.datasources.DataSource$.$anonfun$lookupDataSource$4(DataSource.scala:661)
	at scala.util.Failure.orElse(Try.scala:224)
	at org.apache.spark.sql.execution.datasources.DataSource$.lookupDataSource(DataSource.scala:661)
	... 18 more
