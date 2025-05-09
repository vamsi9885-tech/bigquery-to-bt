25/05/09 11:16:45 INFO SparkEnv: Registering MapOutputTracker
25/05/09 11:16:45 INFO SparkEnv: Registering BlockManagerMaster
25/05/09 11:16:45 INFO SparkEnv: Registering BlockManagerMasterHeartbeat
25/05/09 11:16:45 INFO SparkEnv: Registering OutputCommitCoordinator
25/05/09 11:16:47 INFO DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at gear-bal-cluster-1-m.us-central1-f.c.prj-d-gbl-gar-epgear.internal./172.28.128.10:8032
25/05/09 11:16:47 INFO AHSProxy: Connecting to Application History server at gear-bal-cluster-1-m.us-central1-f.c.prj-d-gbl-gar-epgear.internal./172.28.128.10:10200
25/05/09 11:16:48 INFO Configuration: resource-types.xml not found
25/05/09 11:16:48 INFO ResourceUtils: Unable to find 'resource-types.xml'.
25/05/09 11:16:51 INFO YarnClientImpl: Submitted application application_1746789190206_0001
25/05/09 11:16:52 INFO DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at gear-bal-cluster-1-m.us-central1-f.c.prj-d-gbl-gar-epgear.internal./172.28.128.10:8030
25/05/09 11:16:57 INFO GoogleCloudStorageImpl: Ignoring exception of type GoogleJsonResponseException; verified object already exists with desired state.
25/05/09 11:16:58 INFO GoogleHadoopSyncableOutputStream: hflush(): No-op due to rate limit (RateLimiter[stableRate=0.3qps]): readers will *not* yet see flushed data for gs://prj-d-lumicommon-phs/spark-history/application_1746789190206_0001.inprogress [CONTEXT ratelimit_period="1 MINUTES" ]
Exception in thread "main" java.lang.IllegalAccessError: class com.google.iam.v1.TestIamPermissionsRequest tried to access method 'com.google.protobuf.LazyStringArrayList com.google.protobuf.LazyStringArrayList.emptyList()' (com.google.iam.v1.TestIamPermissionsRequest is in unnamed module of loader org.apache.spark.util.MutableURLClassLoader @363c4251; com.google.protobuf.LazyStringArrayList is in unnamed module of loader 'app')
	at com.google.iam.v1.TestIamPermissionsRequest.<init>(TestIamPermissionsRequest.java:127)
	at com.google.iam.v1.TestIamPermissionsRequest.<clinit>(TestIamPermissionsRequest.java:918)
	at com.google.cloud.bigtable.admin.v2.stub.GrpcBigtableTableAdminStub.<clinit>(GrpcBigtableTableAdminStub.java:304)
	at com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient.create(BigtableTableAdminClient.java:149)
	at com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient.create(BigtableTableAdminClient.java:138)
	at com.example.bqtobt.SparkBQToBT.createBigtableTable(SparkBQToBT.java:85)
	at com.example.bqtobt.SparkBQToBT.main(SparkBQToBT.java:36)
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
