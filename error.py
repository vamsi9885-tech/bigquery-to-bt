25/05/02 16:18:49 INFO GoogleHadoopSyncableOutputStream: hflush(): No-op due to rate limit (RateLimiter[stableRate=0.3qps]): readers will *not* yet see flushed data for gs://prj-d-lumicommon-phs/spark-history/application_1746201557580_0004.inprogress [CONTEXT ratelimit_period="1 MINUTES" ]
usage: post_dataflow.py [-h] [--gear_project_id GEAR_PROJECT_ID]
                        [--temp_path TEMP_PATH]
post_dataflow.py: error: unrecognized arguments: --bq_table_name=triumph_balance --dataset_id=dw --bqQuery = SELECT (`axp-lumid`.dw.decrypt_sde(`axp-lumid`.dw.get_sde_tag('cm13','triumph_balance'),cm13) || '#' || hb9_date_stmt_yr || hb9_date_stmt_mo) as bq_rowkey, hb9_date_stmt_yr,hb9_date_stmt_mo,butl_pct_purch_hi_dpr_am,butl_amt_dlnq_neg_8,butl_amt_adj_ca_fc_cyc FROM `{project_id}.{dataset_id}.{bq_table_name}` WHERE hb9_date_stmt_yr=2024 and hb9_date_stmt_mo='02' LIMIT 1000000  --bqProjectId = axp-lumid
INFO:py4j.clientserver:Closing down clientserver connection
