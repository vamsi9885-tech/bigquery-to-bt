from pyspark.sql.functions import *
from src.utils.df_utils import log_error, log_warn, log_info, drop_table, gen_table_location, run_cmd, partition_udf, delete_hdfs_directory, part_fmt_cd_udf
from pyspark.sql.types import StringType
#####################################################
#File Name: aif_lake_work.py
#Type: Pyspark
#Purpose: To dump data from stage table to lake layer - full/incremental
#Created: 06/07/2018
#Last Updated: 25/10/2019
#Author: Infosys
#####################################################

def stage_lake(input_feed, hc, properties, env, aif_meta, col_list=''):
    """
        Method: stage_lake
        Purpose: To dump data from stage to lake layer - full/incremental
        Arguments: Command line arguments
                :param env : "dev" "QA" "uat" "prod"
                :param input feed: the nth row of input feed list
                :param properties: project specific configuration files from conf/wc_connection.yaml
                :param hc: hive context
                :param sc: spark context
                :param aif_meta: Audit details that needs to be logged
                :param col_list: File ingestion attributes and data type details
    """
    return_code = 1
    if input_feed['load_type'] == 'full':
        return_code = stage_lake_full(input_feed, hc, properties, env, aif_meta, col_list)
    elif input_feed['load_type'] == 'incremental':
        return_code = stage_lake_incremental(input_feed, hc, properties, env, aif_meta)
    return return_code


# lake_partitions	partition_deriving_columns 	lake_partition_logic
# src_ts	extract_prd_id	yyyymmdd|yyyy-mm


def stage_lake_full(input_feed, hc, properties, env, aif_meta, col_list):
    """
        Method: stage_lake_full
        Purpose: To dump data from stage to lake layer
        Arguments: Command line arguments
                :param env : "dev" "QA" "uat" "prod"
                :param input feed: the nth row of input feed list
                :param properties: project specific configuration files from conf/wc_connection.yaml
                :param hc: hive context
                :param aif_meta: Audit details that needs to be logged
                :param col_list: File ingestion attributes and data type details
    """
    return_code = 1
    if input_feed['ingestion_type'] == 'file-based' and input_feed['destination_type'] == 'hive-table':
        if input_feed['lake_db'] != '':
            try:
                db_loc = gen_table_location(input_feed['lake_db'], input_feed['target_table'].lower(), properties[env])
                delete_hdfs_directory(db_loc + "/*", True)
                log_info("LAK DB PATH %s " % db_loc)
                db_lak_table_name = "{db}.{tbl}".format(db=input_feed['lake_db'], tbl=input_feed['target_table'])
                if input_feed['lake_partitions'] != '':
                    # Read lake partition columns from input feed.
                    part_fields = ', '.join(
                        map(lambda part_field: part_field + '  string', input_feed['lake_partitions'].split(',')))
                    # Create DDL for partitioned bucketed/non-bucketed table.    
                    create_ddl_lake = '''create external table if not exists {tbl}({listt}) 
                                      partitioned by ({part_field}) clustered by ({buck_fields})
                                      into {no_buck} buckets stored as {serde} location'{loc}' '''.format(
                        tbl=db_lak_table_name, listt=col_list, loc=db_loc, part_field=part_fields,
                        buck_fields=input_feed['bucket_columns'],
                        no_buck=input_feed['no_of_buckets'],
                        serde=input_feed['lake_storage_type']) \
                        if input_feed['bucket_columns'] != '' \
                        else \
                        '''create external table if not exists {tbl}({listt}) partitioned by ({part_field}) 
                           stored as {serde} location'{loc}' '''.format(
                            tbl=db_lak_table_name, listt=col_list, loc=db_loc, part_field=part_fields, serde=input_feed['lake_storage_type'])
                    create_ddl_lake = create_ddl_lake.replace("|", ",") 
                    log_info("Create_ddl : %s " % create_ddl_lake)
                    if input_feed['lake_partition_logic'] != '':
                        # Partitioned and bucketed table with partition type yyyymmdd|yyyy-mm
                        hc.udf.register('part_udf', partition_udf, returnType=StringType())
                        if input_feed.get('format_code_for_partition_logic', 'N') == 'Y':
                            hc.udf.register('part_udf', part_fmt_cd_udf, returnType=StringType())
                        else:
                            hc.udf.register('part_udf', partition_udf, returnType=StringType())
                        format_from, format_to = input_feed['lake_partition_logic'].split('|')
                        insert_lake = '''INSERT OVERWRITE TABLE {dbtbl} partition({part_fields}) select * from
                               (select *, part_udf({derive_from},'{format_from}','{format_to}') 
                               from {db}.{tbl}) T'''.format(dbtbl=db_lak_table_name, db=input_feed['target_db'],
                               tbl=input_feed['target_table'],
                               part_fields=input_feed['lake_partitions'],
                               derive_from=input_feed['partition_deriving_columns'],
                               format_from=format_from,
                               format_to=format_to)
                    else:
                        # Partitioned and bucketed table without partition type -
                        insert_lake = '''INSERT OVERWRITE TABLE {dbtbl} partition({part_fields}) select * from
                               (select *, {derive_from} from {db}.{tbl}) T'''.format(dbtbl=db_lak_table_name,
                               db=input_feed['target_db'],
                               tbl=input_feed['target_table'],
                               part_fields=input_feed['lake_partitions'],
                               derive_from=input_feed['partition_deriving_columns'])
                # Non Partition table creation and full load
                elif input_feed['lake_partitions'] == '':
                    create_ddl_lake = '''create external table if not exists {tbl}({listt}) stored as {serde} location'{loc}' '''.format(
                        tbl=db_lak_table_name, listt=col_list, loc=db_loc, serde=input_feed['lake_storage_type'])
                    create_ddl_lake = create_ddl_lake.replace("|", ",")
                    log_info("Create_ddl : %s " % create_ddl_lake)
                    insert_lake = '''INSERT OVERWRITE TABLE {dbtbl} select * from {db}.{tbl}'''.format(
                    dbtbl=db_lak_table_name,
                    db=input_feed['target_db'],
                    tbl=input_feed['target_table'])
                log_info("insert_lake : %s " % insert_lake)
                hc.sql("drop table if exists {tbl}".format(tbl=db_lak_table_name))
                try:
                    hc.sql(create_ddl_lake)
                    hc.sql(insert_lake)
                    hc.catalog.refreshTable(db_lak_table_name)
                except Exception as e:
                    log_error(e)
                    log_error("====== Insertion failed for %s ======" %(db_lak_table_name))
                    return_code = 1
                    return return_code

                hive_count = hc.read.table(input_feed['target_db'] + "." + input_feed['target_table']).count()
                t_count = hc.read.table(input_feed['lake_db'] + "." + input_feed['target_table']).count()
                tab_size = run_cmd("hadoop fs -du -h -s " + db_loc)
                bytes_written = tab_size.stdout.split("  ")[0]
                return_code = 0
                aif_meta.target_count = t_count
                aif_meta.data_size_mb = bytes_written
            except Exception as e:
                log_error(e)
                log_error("ERROR WHILE LOADING TO LAKE - %s"% (db_lak_table_name))
                return_code = 1
        else:
            return_code = 0
            aif_meta.status = 'Skipped Lake'
            log_warn('Skipped stage to lake')
    # End of file ingestion to stage
    return return_code


def stage_lake_incremental(input_feed, hc, properties, env, aif_meta):
    """
        Method: stage_lake_incremental
        Purpose: To dump data from stage to lake layer
        Arguments: Command line arguments
                :param env : "dev" "QA" "uat" "prod"
                :param input feed: the nth row of input feed list
                :param properties: project specific configuration files from conf/wc_connection.yaml
                :param hc: hive context
                :param aif_meta: Audit details that needs to be logged
    """
    return_code = 1
    if input_feed['ingestion_type'] == 'file-based' and input_feed['destination_type'] == 'hive-table':
        if input_feed['target_db'] != '' and int(aif_meta.source_count) != 0:
            try:
                partition_columns = input_feed['lake_partitions'].split(',')
                df_lake_table = hc.sql("select * from {lak_db}.{table} where 1=0".format(lak_db=input_feed['lake_db'],
                                                                                         table=input_feed[
                                                                                             'target_table']))
                lake_all_columns = df_lake_table.columns
                # log_info("column list %s " % lake_all_columns)

                # map(lambda partition_column: lake_all_columns.remove(partition_column), partition_columns)
                if input_feed['lake_partitions'] != '':
                    map(lambda partition_column: lake_all_columns.remove(partition_column), partition_columns)
                    if input_feed['lake_partition_logic'] != '':
                        # Partitioned and bucketed table with partition logic yyyymmdd|yyyy-mm
                        if input_feed.get('format_code_for_partition_logic', 'N') == 'Y':
                            hc.udf.register('part_udf', part_fmt_cd_udf, returnType=StringType())
                        else:
                            hc.udf.register('part_udf', partition_udf, returnType=StringType())
                        format_from, format_to = input_feed['lake_partition_logic'].split('|')
                        insert_dml = '''insert into table {db}.{tbl} partition({part_fields}) select * from 
                            (select {col_list}, part_udf({derive_from},'{format_from}','{format_to}') 
                            from {stg_db}.{stg_tbl}) T'''.format(
                            db=input_feed['lake_db'], tbl=input_feed['target_table'],
                            col_list=','.join(lake_all_columns),
                            part_fields=input_feed['lake_partitions'],
                            derive_from=input_feed['partition_deriving_columns'],
                            format_from=format_from,
                            format_to=format_to,
                            stg_db=input_feed['target_db'],
                            stg_tbl=input_feed['target_table'])
                    else:
                        # Partitioned and bucketed table without partition logic
                        insert_dml = '''insert into table {db}.{tbl} partition({part_fields}) select * from 
                        (select {col_list}, {derive_from} from {stg_db}.{stg_tbl}) T'''.format(
                            db=input_feed['lake_db'], tbl=input_feed['target_table'],
                            col_list=','.join(lake_all_columns),
                            part_fields=input_feed['lake_partitions'],
                            derive_from=input_feed['partition_deriving_columns'],
                            stg_db=input_feed['target_db'],
                            stg_tbl=input_feed['target_table'])
                elif input_feed['lake_partitions'] == '':
                    insert_dml = '''insert into table {db}.{tbl} select {col_list} 
                                    from {stg_db}.{stg_tbl}'''.format(
                        db=input_feed['lake_db'], tbl=input_feed['target_table'], col_list=','.join(lake_all_columns),
                        stg_db=input_feed['target_db'],
                        stg_tbl=input_feed['target_table'])
                log_info(insert_dml)
                hc.sql(insert_dml)
                log_info("DATA MIGRATED FROM STAGE TO LAKE FOR TABLE {stg_db}.{stg_tbl}".format(
                    stg_db=input_feed['target_db'], stg_tbl=input_feed['target_table']))
                hc.sql('''refresh table {db}.{tbl}'''.format(db=input_feed['lake_db'], tbl=input_feed['target_table']))
                return_code = 0
                aif_meta.status = 'Lake Success'
            except Exception as e:
                log_error(e)
        else:
            return_code = 0
            aif_meta.status = 'Skipped lake'
            log_warn('Skipped stage to lake Incremental')
        # End of file ingestion to stage
    return return_code
