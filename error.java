25/05/14 12:45:49 INFO SparkEnv: Registering MapOutputTracker
25/05/14 12:45:49 INFO SparkEnv: Registering BlockManagerMaster
25/05/14 12:45:50 INFO SparkEnv: Registering BlockManagerMasterHeartbeat
25/05/14 12:45:50 INFO SparkEnv: Registering OutputCommitCoordinator
25/05/14 12:45:53 INFO DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at gear-bal-cluster-1-m.us-central1-f.c.prj-d-gbl-gar-epgear.internal./172.28.128.5:8032
25/05/14 12:45:53 INFO AHSProxy: Connecting to Application History server at gear-bal-cluster-1-m.us-central1-f.c.prj-d-gbl-gar-epgear.internal./172.28.128.5:10200
25/05/14 12:45:55 INFO Configuration: resource-types.xml not found
25/05/14 12:45:55 INFO ResourceUtils: Unable to find 'resource-types.xml'.
25/05/14 12:45:59 INFO YarnClientImpl: Submitted application application_1747226512705_0001
25/05/14 12:46:00 INFO DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at gear-bal-cluster-1-m.us-central1-f.c.prj-d-gbl-gar-epgear.internal./172.28.128.5:8030
25/05/14 12:46:06 INFO GoogleCloudStorageImpl: Ignoring exception of type GoogleJsonResponseException; verified object already exists with desired state.
25/05/14 12:46:07 INFO GoogleHadoopSyncableOutputStream: hflush(): No-op due to rate limit (RateLimiter[stableRate=0.3qps]): readers will *not* yet see flushed data for gs://prj-d-lumicommon-phs/spark-history/application_1747226512705_0001.inprogress [CONTEXT ratelimit_period="1 MINUTES" ]
table alrady exists
25/05/14 12:46:30 INFO DirectBigQueryRelation: |Querying table prj-d-gbl-gar-epgear.temp._bqc_6872c8c3da56484a836c64c21db958ac, parameters sent from Spark:|requiredColumns=[bq_rowkey,f0_],|filters=[]
25/05/14 12:46:34 INFO ReadSessionCreator: Requested 20000 max partitions, but only received 1 from the BigQuery Storage API for session projects/prj-d-gbl-gar-epgear/locations/us/sessions/CAISDGQzb1RTd0paM0JlSxoCb3YaAm93. Notice that the number of streams in actual may be lower than the requested number, depending on the amount parallelism that is reasonable for the table and the maximum amount of parallelism allowed by the system.
25/05/14 12:46:34 INFO BigQueryRDDFactory: Created read session for table 'prj-d-gbl-gar-epgear.temp._bqc_6872c8c3da56484a836c64c21db958ac': projects/prj-d-gbl-gar-epgear/locations/us/sessions/CAISDGQzb1RTd0paM0JlSxoCb3YaAm93
Exception in thread "main" java.util.NoSuchElementException: key not found: f0_
	at scala.collection.MapLike.default(MapLike.scala:236)
	at scala.collection.MapLike.default$(MapLike.scala:235)
	at scala.collection.AbstractMap.default(Map.scala:65)
	at scala.collection.mutable.HashMap.apply(HashMap.scala:69)
	at com.google.cloud.spark.bigtable.datasources.SchemaMap.getField(BigtableTableCatalog.scala:156)
	at com.google.cloud.spark.bigtable.datasources.BigtableTableCatalog.getField(BigtableTableCatalog.scala:168)
	at com.google.cloud.spark.bigtable.WriteRowConversions.$anonfun$columnIndexAndField$3(WriteRowConversions.scala:50)
	at scala.collection.TraversableLike.$anonfun$map$1(TraversableLike.scala:286)
	at scala.collection.IndexedSeqOptimized.foreach(IndexedSeqOptimized.scala:36)
	at scala.collection.IndexedSeqOptimized.foreach$(IndexedSeqOptimized.scala:33)
	at scala.collection.mutable.ArrayOps$ofRef.foreach(ArrayOps.scala:198)
	at scala.collection.TraversableLike.map(TraversableLike.scala:286)
	at scala.collection.TraversableLike.map$(TraversableLike.scala:279)
	at scala.collection.mutable.ArrayOps$ofRef.map(ArrayOps.scala:198)
	at com.google.cloud.spark.bigtable.WriteRowConversions.<init>(WriteRowConversions.scala:49)
	at com.google.cloud.spark.bigtable.BigtableRelation.insert(BigtableDefaultSource.scala:175)
	at com.google.cloud.spark.bigtable.BigtableDefaultSource.createRelation(BigtableDefaultSource.scala:82)
	at org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand.run(SaveIntoDataSourceCommand.scala:45)
	at org.apache.spark.sql.execution.command.ExecutedCommandExec.sideEffectResult$lzycompute(commands.scala:75)
	at org.apache.spark.sql.execution.command.ExecutedCommandExec.sideEffectResult(commands.scala:73)
	at org.apache.spark.sql.execution.command.ExecutedCommandExec.executeCollect(commands.scala:84)
	at org.apache.spark.sql.execution.QueryExecution$$anonfun$eagerlyExecuteCommands$1.$anonfun$applyOrElse$1(QueryExecution.scala:98)
	at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$6(SQLExecution.scala:109)
	at org.apache.spark.sql.execution.SQLExecution$.withSQLConfPropagated(SQLExecution.scala:169)
	at org.apache.spark.sql.execution.SQLExecution$.$anonfun$withNewExecutionId$1(SQLExecution.scala:95)
	at org.apache.spark.sql.SparkSession.withActive(SparkSession.scala:779)
	at org.apache.spark.sql.execution.SQLExecution$.withNewExecutionId(SQLExecution.scala:64)
	at org.apache.spark.sql.execution.QueryExecution$$anonfun$eagerlyExecuteCommands$1.applyOrElse(QueryExecution.scala:98)
	at org.apache.spark.sql.execution.QueryExecution$$anonfun$eagerlyExecuteCommands$1.applyOrElse(QueryExecution.scala:94)
	at org.apache.spark.sql.catalyst.trees.TreeNode.$anonfun$transformDownWithPruning$1(TreeNode.scala:584)
	at org.apache.spark.sql.catalyst.trees.CurrentOrigin$.withOrigin(TreeNode.scala:176)
	at org.apache.spark.sql.catalyst.trees.TreeNode.transformDownWithPruning(TreeNode.scala:584)
	at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.org$apache$spark$sql$catalyst$plans$logical$AnalysisHelper$$super$transformDownWithPruning(LogicalPlan.scala:30)
	at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformDownWithPruning(AnalysisHelper.scala:267)
	at org.apache.spark.sql.catalyst.plans.logical.AnalysisHelper.transformDownWithPruning$(AnalysisHelper.scala:263)
	at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformDownWithPruning(LogicalPlan.scala:30)
	at org.apache.spark.sql.catalyst.plans.logical.LogicalPlan.transformDownWithPruning(LogicalPlan.scala:30)
	at org.apache.spark.sql.catalyst.trees.TreeNode.transformDown(TreeNode.scala:560)
	at org.apache.spark.sql.execution.QueryExecution.eagerlyExecuteCommands(QueryExecution.scala:94)
	at org.apache.spark.sql.execution.QueryExecution.commandExecuted$lzycompute(QueryExecution.scala:81)
	at org.apache.spark.sql.execution.QueryExecution.commandExecuted(QueryExecution.scala:79)
	at org.apache.spark.sql.execution.QueryExecution.assertCommandExecuted(QueryExecution.scala:116)
	at org.apache.spark.sql.DataFrameWriter.runCommand(DataFrameWriter.scala:860)
	at org.apache.spark.sql.DataFrameWriter.saveToV1Source(DataFrameWriter.scala:390)
	at org.apache.spark.sql.DataFrameWriter.saveInternal(DataFrameWriter.scala:363)
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
