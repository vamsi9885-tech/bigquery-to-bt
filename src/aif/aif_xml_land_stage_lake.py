from pyspark.sql.functions import *
from src.utils.df_utils import *
from src.utils.aif_utils import *
from pyspark.sql.types import *

def xml_loc_land(input_feed, hc, sc, properties, env, aif_meta, file_nm_ptrn):

    return_code = 1
    if input_feed['load_type'] == 'full' or input_feed['load_type'] == 'incremental':
        return_code = 1
        loc_dir = input_feed['local_dir'] if input_feed['local_dir'][-1] == "/" else input_feed['local_dir'] + "/"
        land_dir = input_feed['source_loc'] if input_feed['source_loc'][-1] == "/" else input_feed['source_loc'] + "/"
        arch_dir = input_feed['arch_dir'] if input_feed['arch_dir'][-1] == "/" else input_feed['arch_dir'] + "/"
        run_cmd('hdfs dfs -rm -r -f -safely ' + land_dir + '*')
        format = input_feed['file_format']
        loc_files = get_local_files(loc_dir, file_nm_ptrn, format)
        land_files = land_dir + file_nm_ptrn
        print(loc_files)
        print('before if')
        if len(loc_files):
            print("before put")
            put_hdfs_files(loc_dir+file_nm_ptrn, land_dir)
        aif_meta.status = "Successfully coped to landing zone "
        return_code = 0
        if get_hdfs_files_cnt(land_dir, file_nm_ptrn) != 0 :
            run_cmd("""hdfs dfs -cp "{land_files}" "{arch_dir}" """.format(land_files=land_files, arch_dir=arch_dir))
        aif_meta.status = 'Landing data load Success'
    return return_code


def xml_land_stage(input_feed, hc, sc, properties, env, aif_meta, file_nm_ptrn, col_list):
    return_code = 1
    if input_feed['target_db'] != '':
        if (input_feed['load_type'] == 'full' or input_feed['load_type'] == 'incremental'):
            land_dir = input_feed['source_loc'] if input_feed['source_loc'][-1] == "/" else input_feed['source_loc'] + "/"
            stage_dir = input_feed['source_db'] if input_feed['source_db'][-1] == "/" else input_feed['source_db'] + "/"
            stg_db = input_feed['target_db']
            stg_tbl = input_feed['source_table']
            lak_db = input_feed['lake_db']
            lak_tbl = input_feed['target_table']
            drop_table(stg_db, stg_tbl, hc, properties[env], True)
            land_files = land_dir + file_nm_ptrn
            row_tag = input_feed['row_tag']
            if get_hdfs_files_cnt(land_dir, file_nm_ptrn) != 0:
                df = hc.read.format('com.databricks.spark.xml').option("rowTag", row_tag).load(land_files)\
                    .withColumn('file_name', split(input_file_name(),"/")[size(split(input_file_name(),"/"))-1])\
                    .withColumn('access_time', lit(None)) \
                    .withColumn('modified_time', lit(None)) \
                    .withColumn('load_dt', current_timestamp())

                schema = get_df_schema(hc, df) if col_list == '' else col_list + ", file_name string, access_time string, modified_time string, load_dt string"

                stg_tbl_loc = get_table_location(input_feed['target_db'], input_feed['target_table'], properties[env])
                log_info('''create external table {db}.{tbl}({schema}) stored as parquet location '{loc}' '''
                       .format(db=stg_db, tbl=stg_tbl, schema=schema,
                               loc=stg_tbl_loc))
                hc.sql('''create external table {db}.{tbl}({schema}) stored as parquet location '{loc}' '''
                       .format(db=stg_db, tbl=stg_tbl, schema=schema,
                               loc=stg_tbl_loc))

                stg_columns = hc.read.table(stg_db+'.'+stg_tbl).columns

                df.select(*stg_columns).registerTempTable("df")

                hc.sql('''insert overwrite table {db}.{tbl} select * from df'''.format(db=input_feed['target_db'],
                                                                                       tbl=input_feed['target_table']))
                hc.sql('refresh table {db}.{tbl}'.format(db=input_feed['target_db'], tbl=input_feed['target_table']))
                hive_count = hc.read.table(
                    '{db}.{tbl}'.format(db=input_feed['target_db'], tbl=input_feed['target_table'])).count()
                ret_size = run_cmd('hadoop fs -du -h -s {loc}'.format(loc=stg_tbl_loc))
                bytes_written = ret_size.stdout.split("  ")[0]
                aif_meta.target_count = hive_count
                aif_meta.data_size_mb = bytes_written
                return_code = 0
                aif_meta.status = 'Stage Success'
            else:
                return_code = 0
                log_info('Zero stage files to load')
                aif_meta.status = 'Stage Success'
        else:
            log_error('Incorrect load type, please check the input feed')
    else:
        return_code = 0
        aif_meta.status = 'Skipped Stage Load'
    return return_code



def xml_stage_lake(input_feed, hc, sc, properties, env, aif_meta, col_list):
    stg_db = input_feed['target_db']
    stg_tbl = input_feed['source_table']
    lak_db = input_feed['lake_db']
    lak_tbl = input_feed['target_table']
    lak_tbl_loc = get_table_location(lak_db, lak_tbl, properties[env])
    return_code = 1
    if input_feed['lake_db'] != '' and hc._jsparkSession.catalog().tableExists(stg_db,stg_tbl):
        if input_feed['load_type'] == 'full':
            drop_table(lak_db, lak_tbl, hc, properties[env], True)
            df = hc.read.table(stg_db+'.'+stg_tbl)

            schema = get_df_schema(hc, df) if col_list == '' else col_list + ", file_name string, access_time string, modified_time string, load_dt string"

            hc.sql('''create external table {db}.{tbl}({schema}) stored as parquet location '{loc}' '''
                   .format(db=lak_db, tbl=lak_tbl, schema=schema,
                           loc=lak_tbl_loc))
            lake_columns = hc.read.table(lak_db+'.'+lak_tbl).columns
            df.select(*lake_columns).registerTempTable("df")
            hc.sql('''insert overwrite table {db}.{tbl} select * from df'''.format(db=lak_db,
                                                                                   tbl=lak_tbl))
            hc.sql('refresh table {db}.{tbl}'.format(db=lak_db, tbl=lak_tbl))
            hive_count = hc.read.table(
                '{db}.{tbl}'.format(db=lak_db, tbl=lak_tbl)).count()
            ret_size = run_cmd('hadoop fs -du -h -s {loc}'.format(loc=lak_tbl_loc))
            bytes_written = ret_size.stdout.split("  ")[0]
            aif_meta.target_count = hive_count
            aif_meta.data_size_mb = bytes_written
            return_code = 0
            aif_meta.status = 'Lake Success'
        elif input_feed['load_type'] == 'incremental':
            df = hc.read.table(stg_db + '.' + stg_tbl)

            schema = get_df_schema(hc, df) if col_list == '' else col_list + ", file_name string, access_time string, modified_time string, load_dt string"

            lak_df = hc.sql("select * from {db}.{tbl} where 1=0".format(db = lak_db, tbl = lak_tbl))
            lake_schema = get_df_schema(hc, lak_df)
            log_info(lake_schema)
            log_info(schema)
            df_lake_files = hc.sql("select distinct file_name from {db}.{tbl}".format(db=lak_db, tbl=lak_tbl))
            df_result= df.alias('stg').join(df_lake_files.alias('lak'), (col('stg.file_name') == col('lak.file_name')),
                                 "left_outer").filter(col('lak.file_name').isNull()).drop(col('lak.file_name'))

            #s_schema = map(lambda x: x.strip().loder(), schema.split(',')).sor()
            #l_schema = map(lambda x: x.strip().loder(), lake_schema.split(',')).sor()
            s_schema = df.columns.sort()
            l_schema = lak_df.columns.sort()
            if s_schema == l_schema:
                df_result.registerTempTable("df")
                hc.sql('''insert into table {db}.{tbl} select * from df'''.format(db=lak_db,
                                                                                       tbl=lak_tbl))
                hc.sql('refresh table {db}.{tbl}'.format(db=lak_db, tbl=lak_tbl))
                hive_count = hc.read.table(
                    '{db}.{tbl}'.format(db=lak_db, tbl=lak_tbl)).count()
                ret_size = run_cmd('hadoop fs -du -h -s {loc}'.format(loc=lak_tbl_loc))
                bytes_written = ret_size.stdout.split("  ")[0]
                aif_meta.target_count = hive_count
                aif_meta.data_size_mb = bytes_written
                return_code = 0
                aif_meta.status = 'Lake Success'
            else:
                log_error("Stage schema is different than the Lake schema, incremental data can't "
                          "be loaded to the existing lake table")
    else:
        return_code = 0
        aif_meta.status = 'Skipped Lake Load'
    return return_code