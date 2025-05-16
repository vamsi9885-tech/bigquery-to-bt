from src.run.create_spark_session import *
from src.utils.df_utils import *
from src.aif.aif_db_stage import *
from src.aif.aif_stage_lake import *
from src.aif.aif_meta import *
from src.utils.df_utils import Result, date_time_now, aif_meta_write
from datetime import datetime, date
from pyspark.sql.types import StringType,StructField, StructType

import sys,os,re

""" function removed
def get_hdfs_files(sc, input_feed, delta_time_obj):
    try:
        if input_feed.get('is_local_file', 'N') == 'Y':
            ex_code = csv_importer(input_feed, delta_time_obj)
            if ex_code != 0: return None
        hadoop = sc._jvm.org.apache.hadoop
        fs = hadoop.fs.FileSystem
        conf = hadoop.conf.Configuration()
        source_loc = input_feed['control_loc']
        path = hadoop.fs.Path(source_loc)
        paths = map(lambda x: str(x.getPath()), fs.get(conf).listStatus(path))
        source_file_name = input_feed['control_file']
        files = filter(lambda x: source_loc + "/" + source_file_name in x, paths)
        return files
    except Exception as e:
        log_error("ERROR retreiving files from HDFS")
        log_error(e)
"""


def txt_file_stage(input_feed, hc, sc, properties, env, aif_meta, col_list=''):
    """
        Method: db_stage
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
        '''No selected columns, handle it in later stage'''
        return_code = txt_file_stage_full(input_feed, hc, sc, properties, env, aif_meta, col_list)
    elif input_feed['load_type'] == 'incremental':
        '''for file ingestion stage should be always fully loaded and overwriten'''
        return_code = txt_file_stage_full(input_feed, hc, sc, properties, env, aif_meta, col_list)
    return return_code

def txt_file_stage_full(input_feed, hc, sc, properties, env, aif_meta, col_list):
    """
        Method: db_stage_full
        Purpose: To ingest entire data from source table
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

    try:
        db_stg_table_name = "{db}.{tbl}".format(db=input_feed['target_db'], tbl=input_feed['target_table'].lower())
        db_loc = get_table_location(input_feed['target_db'], input_feed['target_table'], properties[env])
        log_info("STG DB PATH %s " % db_loc)
        header = True if input_feed['header_avail'].lower().strip() == "true" else False
        col_names = map(lambda x: x.split(" ")[0], col_list.split(",")) #'''Make a list of columns'''
        l = len(col_names)
        source_loc = input_feed["source_loc"].replace(" ","")
        patterns = [r'\W+']
        for p in patterns:
            match = re.findall(p,source_loc)
        if len("".join(set(match))) != 1:
            log_error('input_feed["source_loc"] {source_loc} is not correct'.format(source_loc=source_loc))
            return return_code
        source_loc = source_loc if source_loc.startswith("/") else "/" + source_loc
        source_loc = source_loc[0:-1] if source_loc.endswith("/") else source_loc
        '''get the list of files'''
        files = get_local_file_list(source_loc,input_feed['source_file'],format=input_feed['file_format'].lower()) if(input_feed['local_file'].lower()=="y") else get_hdfs_file_list(sc,source_loc,input_feed['source_file'])
        '''Loop each file and add the '''
        df_load = None #accumulate all records in this data frame
        for file_item in files:
            file = "file://"+file_item[1] if(input_feed['local_file'].lower()=="y") else file_item[1];file_name = file_item[0];at = file_item[2];mt=file_item[3];file_size = file_item[4]
            '''if file_size is 0 then go to next file, this will also remove any directory from list'''
            log_info(str([file_name, file, at, mt, file_size]))
            if file_size == '0': continue
            df_load_t_count = 0 #init
            try:
                if input_feed.get('enable_schema',False):
                    # only support for the specified schema to skip the invalid data
                    schema = generate_schema_without_load_dt(col_list)
                    df_load_t = hc.read.load(file,
                                format=input_feed['file_format'].lower(),
                                sep=input_feed['delimiter'], header=header,
                                schema = schema, multiLine=input_feed.get('is_multiline', True),
                                timestampFormat="yyyy/MM/dd HH:mm:ss ZZ", escape='"'
                                )
                    df_load_t_count = df_load_t.count()
                else:
                    df_load_t = hc.read.options(
                        format=input_feed['file_format'].lower(),
                        sep=input_feed['delimiter'], header=header,
                        timestampFormat="yyyy/MM/dd HH:mm:ss ZZ", escape='"').csv(file)
                    '''change the column names'''
                    if df_load_t is None : continue
                    df_load_t_count = df_load_t.count()
                    if df_load_t_count == 0 : continue
                    # replace the hive table key words
                    df_load_t = replace_keywords_header(df_load_t, df_load_t.columns[:l])
                    file_col_list = df_load_t.columns[:l]
                    df_load_t = df_load_t.select(*file_col_list)
                    if l != len(df_load_t.columns): continue
                    # take first n number of columns where n is the size of col_names
                    df_load_t = df_load_t.select([col(x[0]).alias(x[1]) for x in zip(file_col_list, col_names)])
                '''Load the table'''
                """ add File_name, Access Time, Modify time columns and file names """
                if input_feed.get('no_filename', 0) == 0:  df_load_t = df_load_t.withColumn("file_name", lit(file_name))
                if input_feed.get('no_access_time', 0) == 0: df_load_t = df_load_t.withColumn("access_time", lit(at))
                if input_feed.get('no_modify_time', 0) == 0: df_load_t = df_load_t.withColumn("modified_time", lit(mt))
            except Exception as e:
                log_error(e)
                log_error("====== FILE LOADING FAILED %s ======" % file_name)
            skip_n =int(input_feed.get('skip_rows',0))
            if 0 < skip_n < df_load_t_count:
                skip_df = hc.createDataFrame(df_load_t.head(skip_n), df_load_t.schema)
                df_load_t = df_load_t.subtract(skip_df)
                df_load_t_count = df_load_t.count()
            if df_load_t_count > 0:
                df_load = df_load_t if df_load is None else df_load.union(df_load_t)
        '''End looping files'''

        '''Add load_dt'''
        if df_load is not None and df_load.count() > 0:
            df_load = df_load.withColumn("load_dt", current_timestamp())
            '''Add new columns to column list'''
            if input_feed.get('no_filename', 0) == 0:col_list = col_list + ',file_name string'
            if input_feed.get('no_access_time', 0) == 0: col_list = col_list + ',access_time string'
            if input_feed.get('no_modify_time', 0) == 0:col_list = col_list + ',modified_time string'
            col_list = col_list + ',load_dt string'
            """Masking Code Starts"""
            if input_feed.get('mask_columns', 0) != 0:
                df_load = generate_sql_masking_df(input_feed, properties[env], hc, sc, df_load)
            """Masking Code ends"""
            aif_meta.source_count = df_load.count()
            log_info("Total source row count: "+ str(aif_meta.source_count))
            drop_stmt = "drop table if exists {tbl}".format(tbl=db_stg_table_name)
            hc.sql(drop_stmt)
            create_ddl_stage = "create external table if not exists {tbl} ({col_list}) stored as " \
                               "parquet location '{loc}'".format(tbl=db_stg_table_name, col_list=col_list, loc=db_loc)
            insert_stage = "INSERT OVERWRITE TABLE {tbl} select * from tmp_stg_file_ingest".format(
                tbl=db_stg_table_name)
            try:
                hc.sql(create_ddl_stage)
                df_load = df_load.fillna('')
                df_load.registerTempTable("tmp_stg_file_ingest")
                hc.sql(insert_stage)
                hc.catalog.refreshTable(db_stg_table_name)
            except Exception as e:
                log_error(e)
                log_error("====== INSERTION FAILED FOR %s ======" % (db_stg_table_name))
                return_code = 1
                return return_code

            log_info(create_ddl_stage)
            log_info(insert_stage)
            log_info("hdfs_to_stage done")

            hive_count = df_load.count()
            log_info("Data from row count: "+str(hive_count))
            tab_size = run_cmd("hadoop fs -du -h -s " + db_loc)
            bytes_written = tab_size.stdout.split("  ")[0]
            return_code = 0
            aif_meta.target_count = hive_count
            aif_meta.data_size_mb = bytes_written
            aif_meta.status = 'Ref Stage Success'
        else:
            log_error("NO COLUMN/NO DATA FOUND - EXITING WITHOUT CREATING TABLE")
    except Exception as e:
        log_error("NO COLUMN DETAILS/ FILE NOT FOUND - EXITING WITHOUT CREATING TABLE")
        log_error(e)
        return_code = 1
# End of file ingestion to stage

    return return_code

def replace_keywords_header(df, header_list):
    pattern = r"[\w+]"
    for header in header_list:
            new_header = "".join(re.findall(pattern,header))
            df = df.withColumnRenamed(header, new_header)
    return  df


def generate_schema_without_load_dt(col_list):
    try:
        type_dict = {
            'string': StringType(),
        }
        col_list = col_list.split(",")
        col_list = map(lambda x: x.lower(), col_list)
        struct_fields = map(lambda x: StructField(x.split(" ")[0], type_dict.get(x.split(" ")[1], StringType()), True),
                            col_list)
        schema = StructType(struct_fields)
        return schema
    except Exception as e:
        log_error("ERROR generating schema from dic")
        log_error(e)