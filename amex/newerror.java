25/05/16 12:40:29 INFO SparkBQToBT: used arguments : {instance_id=lumiplfeng-decppgusbt241030202635, json_file=gear_hist_finapi/sample_testing/bq2bt_otl_mapping/bal_otl2_map.json, project_id=axp-lumid, bt_project_name=prj-d-lumi-bt, dataset_id=dw, gear_project_id=prj-d-gbl-gar-epgear}
25/05/16 12:40:29 INFO SparkBQToBT: initializing spark session............
25/05/16 12:40:33 INFO SparkEnv: Registering MapOutputTracker
25/05/16 12:40:33 INFO SparkEnv: Registering BlockManagerMaster
25/05/16 12:40:33 INFO SparkEnv: Registering BlockManagerMasterHeartbeat
25/05/16 12:40:34 INFO SparkEnv: Registering OutputCommitCoordinator
25/05/16 12:40:37 INFO DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at gear-trans-cluster-2-m.us-central1-f.c.prj-d-gbl-gar-epgear.internal./172.28.128.57:8032
25/05/16 12:40:38 INFO AHSProxy: Connecting to Application History server at gear-trans-cluster-2-m.us-central1-f.c.prj-d-gbl-gar-epgear.internal./172.28.128.57:10200
25/05/16 12:40:40 INFO Configuration: resource-types.xml not found
25/05/16 12:40:40 INFO ResourceUtils: Unable to find 'resource-types.xml'.
25/05/16 12:40:45 INFO YarnClientImpl: Submitted application application_1747399007209_0001
25/05/16 12:40:46 INFO DefaultNoHARMFailoverProxyProvider: Connecting to ResourceManager at gear-trans-cluster-2-m.us-central1-f.c.prj-d-gbl-gar-epgear.internal./172.28.128.57:8030
25/05/16 12:40:54 INFO GoogleCloudStorageImpl: Ignoring exception of type GoogleJsonResponseException; verified object already exists with desired state.
25/05/16 12:40:55 INFO GoogleHadoopSyncableOutputStream: hflush(): No-op due to rate limit (RateLimiter[stableRate=0.3qps]): readers will *not* yet see flushed data for gs://prj-d-lumicommon-phs/spark-history/application_1747399007209_0001.inprogress [CONTEXT ratelimit_period="1 MINUTES" ]
25/05/16 12:41:00 INFO SparkBQToBT: starting Bigquery to Bigtable process
25/05/16 12:41:02 INFO SparkBQToBT: catalog Structure : {"table":{"name":"gear_triumph_transactions_2","tableCoder":"PrimitiveType"},"rowkey":"bq_rowkey","columns":{"bq_rowkey":{"cf":"rowkey","col":"bq_rowkey","type":"string"},"date_stmt_yr":{"cf":"tr","col":"aa","type":"string"},"date_stmt_mo":{"cf":"tr","col":"ab","type":"string"},"cm13":{"cf":"tr","col":"ac","type":"string"},"decrypt_cm13":{"cf":"tr","col":"ac","type":"string"},"se10":{"cf":"tr","col":"ad","type":"string"},"code_trans":{"cf":"tr","col":"ae","type":"string"},"time_add":{"cf":"tr","col":"af","type":"string"},"time_add_dttm":{"cf":"tr","col":"ag","type":"string"},"date_stmt_dy":{"cf":"tr","col":"ah","type":"string"},"date_trans_1":{"cf":"tr","col":"ai","type":"string"},"amt_trans":{"cf":"tr","col":"aj","type":"string"},"addr_state":{"cf":"tr","col":"ak","type":"string"},"date_post":{"cf":"tr","col":"al","type":"string"},"nbr_offr_terms_id":{"cf":"tr","col":"am","type":"string"},"rfrn_mrkt_offr_id":{"cf":"tr","col":"an","type":"string"},"cm15":{"cf":"tr","col":"ao","type":"string"},"decrypt_cm15":{"cf":"tr","col":"ao","type":"string"},"rfrn_majr_inds_cd":{"cf":"tr","col":"ap","type":"string"},"rfrn_sub_inds_cd":{"cf":"tr","col":"aq","type":"string"},"rfrn_batch_nbr":{"cf":"tr","col":"ar","type":"string"},"nbr_sub_code":{"cf":"tr","col":"as","type":"string"},"desc_trans":{"cf":"tr","col":"at","type":"string"},"indc_cr_db":{"cf":"tr","col":"au","type":"string"},"code_prod_nbr":{"cf":"tr","col":"av","type":"string"},"code_acct_cyc_day":{"cf":"tr","col":"aw","type":"string"},"code_acct_stat":{"cf":"tr","col":"ax","type":"string"},"nbr_plyr_acct":{"cf":"tr","col":"ay","type":"string"},"code_pr_po_fin":{"cf":"tr","col":"az","type":"string"},"nbr_sub_prod_1":{"cf":"tr","col":"ba","type":"string"},"rfrn_capt_cntr_nbr":{"cf":"tr","col":"bb","type":"string"},"code_mrkt_offr_typ":{"cf":"tr","col":"bc","type":"string"},"indc_be_loc_xfer":{"cf":"tr","col":"bd","type":"string"},"code_adj_ssp_pymt":{"cf":"tr","col":"be","type":"string"},"nme_serv_estb":{"cf":"tr","col":"bf","type":"string"},"indst_prch_code":{"cf":"tr","col":"bg","type":"string"},"code_srce_ctry":{"cf":"tr","col":"bh","type":"string"},"trans_refer_no":{"cf":"tr","col":"bi","type":"string"}}}
Exception in thread "main" java.lang.IllegalAccessError: class com.google.iam.v1.TestIamPermissionsRequest tried to access method 'com.google.protobuf.LazyStringArrayList com.google.protobuf.LazyStringArrayList.emptyList()' (com.google.iam.v1.TestIamPermissionsRequest is in unnamed module of loader org.apache.spark.util.MutableURLClassLoader @7afc4db9; com.google.protobuf.LazyStringArrayList is in unnamed module of loader 'app')
	at com.google.iam.v1.TestIamPermissionsRequest.<init>(TestIamPermissionsRequest.java:127)
	at com.google.iam.v1.TestIamPermissionsRequest.<clinit>(TestIamPermissionsRequest.java:918)
	at com.google.cloud.bigtable.admin.v2.stub.GrpcBigtableTableAdminStub.<clinit>(GrpcBigtableTableAdminStub.java:304)
	at com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient.create(BigtableTableAdminClient.java:149)
	at com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient.create(BigtableTableAdminClient.java:138)
	at com.example.bqtobt.SparkBQToBT.createBigtableTable(SparkBQToBT.java:91)
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
