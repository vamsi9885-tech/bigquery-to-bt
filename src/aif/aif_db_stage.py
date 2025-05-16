from src.utils.aif_utils import validate_header, csv_importer
from src.utils.df_utils import Result, date_time_now
from pyspark.sql.functions import lit
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, StringType, DoubleType, BooleanType, StructField, StructType, TimestampType , DecimalType , ShortType
from src.utils.df_utils import delete_hdfs_directory
from datetime import datetime
import time, heapq
from src.utils.df_utils import run_cmd, log_error, log_info, log_warn, get_table_location, drop_table


#####################################################
# File Name: aif_db_stage.py
# Type: Pyspark
# Purpose: To ingest data from rdbMS to stage layer - full/incremental
#        Also, to ingest files from HDFS landing location to stage layer
# Created: 06/07/2018
# Last Updated: 25/10/2019
# Author: Infosys
#####################################################


def generate_schema(col_list):
    try:
        type_dict = {
            'int': IntegerType(),
            'string': StringType(),
            'boolean': BooleanType(),
            'double': DoubleType(),
            'bigint': IntegerType(),
            'timestamp': TimestampType(),
            'decimal(38|10)':  DecimalType(38,10),
            'decimal(13|5)':  DecimalType(13,5),
            'decimal(15|2)':  DecimalType(15,2),
            'smallint' : ShortType()
        }
        col_list = col_list.split(",")
        col_list.remove("load_dt string")
        col_list = map(lambda x: x.lower(), col_list)
        struct_fields = map(lambda x: StructField(x.split(" ")[0], type_dict.get(x.split(" ")[1], StringType()), True),
                            col_list)
        schema = StructType(struct_fields)
        return schema
    except Exception as e:
        log_error("ERROR generating schema from dic")
        log_error(e)


def get_hdfs_files(sc, input_feed):
    try:
        if input_feed.get('is_local_file', 'N') == 'Y':
            ex_code = csv_importer(input_feed)
            if ex_code != 0: return None
        hadoop = sc._jvm.org.apache.hadoop
        fs = hadoop.fs.FileSystem
        conf = hadoop.conf.Configuration()
        source_loc = input_feed.get('control_loc','')
        source_file_name = input_feed.get('control_file','')
        if input_feed.get('is_control_file', 'Y') == 'N':
            source_loc = input_feed['source_db']
            source_file_name = input_feed['source_table']
        path = hadoop.fs.Path(source_loc)
        paths = map(lambda x: str(x.getPath()), fs.get(conf).listStatus(path))
        if input_feed.get('is_prefix', "N") == "Y":
            files = filter(lambda x: x.endswith(source_file_name + input_feed.get("file_extension", ".csv")), paths)
        else:
            files = filter(lambda x: source_loc + "/" + source_file_name in x, paths)
        return files
    except Exception as e:
        log_error("ERROR retreiving files from HDFS")
        log_error(e)


def get_latest_files(sc, input_feed, delta_time_obj=None):
    """
            Method: get_latest_files
            Arguments:
                    :param sc : sparkContext
                    :param input feed: the nth row of input feed list
                    :param delta_time_obj: load_dt from the Table
        """
    try:
        file_list = list()
        source_file_name = input_feed.get('control_file','')
        if input_feed.get('is_control_file', 'Y') == 'N':
            source_file_name = input_feed['source_table']
        timestamp_format = input_feed.get("timestamp_format", "%Y%m%d")
        files = get_hdfs_files(sc, input_feed)
        if delta_time_obj is not None:
            for f in files:
                index_start = f.rfind('/') + 1
                index_end = f.rfind(".")
                file_name = f[index_start:index_end]
                file_timestamp = file_name.replace(source_file_name, '')
                date_of_creation = datetime.strptime(file_timestamp, timestamp_format)
                if date_of_creation > delta_time_obj:
                    file_list.append(f)
            return file_list
        else:
            if input_feed.get('is_multi_files', 'Y') == 'Y' or input_feed.get('is_timestamp_suffix', 'Y') == 'N':
                file_list = files
            else:
                for f in files:
                    index_start = f.rfind('/') + 1
                    index_end = f.rfind(".")
                    file_name = f[index_start:index_end]
                    file_timestamp = file_name.replace(source_file_name, '')
                    date_of_creation = datetime.strptime(file_timestamp, timestamp_format)
                    file_list.append(date_of_creation)
                max_date = heapq.nlargest(1, file_list)
                max_date = datetime.strftime(max_date[0], timestamp_format)
                if input_feed.get('is_prefix', 'N') == 'Y':
                    file_list = filter(lambda x: max_date + source_file_name in x, files)
                else:
                    file_list = filter(lambda x: source_file_name + max_date in x, files)
            return file_list
    except Exception as e:
        log_error("ERROR in retrieving incremental files")
        log_error(e)


def db_stage(input_feed, hc, sc, properties, env, aif_meta, col_list=''):
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
        return_code = db_stage_full(input_feed, hc, sc, properties, env, aif_meta, col_list)
    elif input_feed['load_type'] == 'incremental':
        return_code = db_stage_incremental(input_feed, hc, sc, properties, env, aif_meta, col_list)
    return return_code


def generate_sql_masking_query(masking_columns, normal_columns, hashing_column, date_column, jumble_column, target_tbl):
    sql_query = list()
    sql_query.append("SELECT ")

    """Replace the first two chars into 'XX'"""
    map(lambda x: sql_query.append("mask_first(trim(" + str(x) + "),2) as " + str(x) + ","), masking_columns)
    """Hash the column value"""
    map(lambda x: sql_query.append("mask_hash(trim(" + str(x) + ")) as " + str(x) + ","), hashing_column)
    """Trim all the fields to remove the extra paded spaces"""
    map(lambda x: sql_query.append("trim(" + str(x) + ") as " + str(x) + ","), normal_columns)
    """Convert the source date field to 'yyyy-MM-dd' format, replace the day and month as '01-01' and convert again to ource format"""
    map(lambda x: (lambda a=str(x).split("|")[
        0].lower().strip(), b=str(x).split("|")[
        1].strip(): sql_query.append(
        "nvl(from_unixtime(unix_timestamp(concat(year(from_unixtime(unix_timestamp(nvl(" + a + ",'') , '" + b + "'),'yyyy-MM-dd')),'-01-01'),'yyyy-MM-dd'),'" + b + "'),'') as " + a + ","))(),
        date_column)
    """Check if the length is greater than or equal to 5.if yes, find the hash value of last five chars and repace it with the last five chars of the hash value, else hash the value and replace it with last five chars of the hash value. Handling null and '' values as well."""
    map(lambda x: sql_query.append(
        "if(length(trim(" + str(x) + ")) >= 5,concat_ws('',substr(nvl(trim(" + str(x) + "),''),0,instr(nvl(trim(" + str(
            x) + "),''),substr(trim(" + str(
            x) + "),-5))-1),substr(custom_hash(substr(trim(" + str(x) + "),-5)),-5)),if(length(trim(" + str(
            x) + ")) > 0,custom_hash(trim(" + str(x) + ")),''))" + " as " + x + ","), jumble_column)

    sql_query = ''.join(sql_query)[:-1] + " FROM " + target_tbl
    return sql_query


def generate_sql_query(normal_columns, target_tbl):
    sql_query = list()
    sql_query.append("SELECT ")

    """Trim all the fields to remove the extra paded spaces"""
    map(lambda x: sql_query.append("trim(" + str(x) + ") as " + str(x) + ","), normal_columns)
    sql_query = ''.join(sql_query)[:-1] + " FROM " + target_tbl
    return sql_query


def generate_sql_masking_df(input_feed, env, hc, sc, df):
    if input_feed['ingestion_type'] == 'file-based':
        columns = df.columns
    else:
        columns = get_rdbms_tbl_columns(input_feed, env, hc, sc)
    columns = map(lambda x: x.lower(), columns)
    if input_feed.get('mask_columns'):
        masking_column = map(lambda x: x.lower().strip(),
                             filter(lambda x: x.startswith('M:'), input_feed['mask_columns'].split("~"))[0][2:].split(
                                 ",")) if len(
            filter(lambda x: x.startswith('M:'), input_feed['mask_columns'].split("~"))) > 0 else list()

        hashing_column = map(lambda x: x.lower().strip(),
                             filter(lambda x: x.startswith('H:'), input_feed['mask_columns'].split("~"))[0][2:].split(
                                 ",")) if len(
            filter(lambda x: x.startswith('H:'), input_feed['mask_columns'].split("~"))) > 0 else list()

        date_column = map(lambda x: x.strip(),
                          filter(lambda x: x.startswith('D:'), input_feed['mask_columns'].split("~"))[0][2:].split(
                              ",")) if len(
            filter(lambda x: x.startswith('D:'), input_feed['mask_columns'].split("~"))) > 0 else list()

        date_attr = map(lambda x: x.split("|")[0].lower().strip(), date_column)

        jumble_column = map(lambda x: x.lower().strip(),
                            filter(lambda x: x.startswith('J:'), input_feed['mask_columns'].split("~"))[0][2:].split(
                                ",")) if len(
            filter(lambda x: x.startswith('J:'), input_feed['mask_columns'].split("~"))) > 0 else list()

        normal_columns = list(
            set(columns) - set(masking_column) - set(hashing_column) - set(date_attr) - set(jumble_column))
        sql_query_mask = generate_sql_masking_query(masking_column, normal_columns, hashing_column, date_column,
                                                    jumble_column,
                                                    "mask_tbl")
    else:
        sql_query_mask = generate_sql_query(columns, "mask_tbl")

    log_info(sql_query_mask)
    df.registerTempTable("mask_tbl")
    df = hc.sql(sql_query_mask).select(*columns)
    hc.catalog.dropTempView("mask_tbl")
    return df


def db_stage_full(input_feed, hc, sc, properties, env, aif_meta, col_list):
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
    if input_feed['ingestion_type'] == 'file-based':
        try:
            db_stg_table_name = "{db}.{tbl}".format(db=input_feed['target_db'], tbl=input_feed['target_table'].lower())
            db_loc = get_table_location(input_feed['target_db'], input_feed['target_table'], properties[env])
            log_info("STG DB PATH %s " % db_loc)
            files_read = get_latest_files(sc, input_feed)
            if not files_read:
                aif_meta.status = 'Load Failed'
                log_error('No files for full load')

                return 1
            header = True if input_feed['header_avail'].lower().strip() == "true" else False
            select_columns = True if input_feed['select_columns'].lower().strip() == "true" else False
            delimiter=input_feed.get('delimiter', ',')
            col_names = map(lambda x: x.strip().split(" ")[0].strip(), col_list.split(","))
            col_names.remove("load_dt")
            log_info("col_names : %s" % col_names)
            schema = generate_schema(col_list)
            df_load = hc.createDataFrame(hc.sparkContext.emptyRDD(), schema)
            for file in files_read:
                if input_feed.get('is_control_file', 'Y') == 'Y':
                    file = file.replace(input_feed['control_loc'] + "/" + input_feed['control_file'],
                                    input_feed['source_db'] + "/" + input_feed['source_table'])
                if header:
                    special_char = input_feed.get('special_chars_in_column', '')
                    file_encoding_frmt = input_feed.get('file_encoding_frmt', '')
                    header_val = validate_header(sc, col_names, file, delimiter, select_columns, special_char,file_encoding_frmt)
                    if header_val == 1:
                        log_warn('Incorrect header encountered')
                        return header_val

                if input_feed.get('is_schema', 'FALSE') == 'TRUE':

                    df_load_f = hc.read.load(file,
                                             format=input_feed['file_format'].lower(),
                                             sep=delimiter, header=header, schema=schema,
                                             timestampFormat="yyyy/MM/dd HH:mm:ss ZZ",
                                             escape=input_feed.get('escape_char', '"'),
                                             quote=input_feed.get('quote_char', '"'),
                                             multiLine=input_feed.get('is_multiline', False))
                else:
                    df_load_f = hc.read.load(file,
                                             format=input_feed['file_format'].lower(),
                                             sep=delimiter, header=header,
                                             timestampFormat="yyyy/MM/dd HH:mm:ss ZZ",
                                             escape=input_feed.get('escape_char', '"'),
                                             quote=input_feed.get('quote_char', '"'),
                                             multiLine=input_feed.get('is_multiline', False))

                if not header:
                    col_names_nohead = []
                    for x in col_list.split(','):
                        name, value = x.split(" ")
                        col_names_nohead.append(name)
                    log_info("col_names_nohead. %s" % col_names_nohead)
                    col_names_nohead.remove("file_name")
                    col_names_nohead.remove("load_dt")
                    df_load_f = df_load_f.toDF(*col_names_nohead)
                flag = input_feed.get("extract_time")
                if flag is not None and flag.lower()=="true":
                    path = lambda p: sc._jvm.org.apache.hadoop.fs.Path(p)
                    fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
                    time_ts = fs.getFileStatus(path(file)).getModificationTime()
                    creation_time = time.strftime("%Y-%m-%d %H:%M:%S.%s", time.localtime(time_ts / 1000))[:-7]
                    df = df_load_f.withColumn("event_file_date", lit(str(creation_time))).select(*col_names)
                else:
                    df = df_load_f.withColumn("file_name", lit(file[file.rfind('/') + 1:])) \
                        .select(*col_names)
                df_load = df_load.unionAll(df)
            df_load = df_load.withColumn("load_dt", current_timestamp())
            """Masking Code Starts"""
            if input_feed.get('mask_columns', 0) != 0:
                df_load = generate_sql_masking_df(input_feed, properties[env], hc, sc, df_load)
            """Masking Code ends"""
            aif_meta.source_count = df_load.count()
            drop_stmt = "drop table if exists {tbl}".format(tbl=db_stg_table_name)
            hc.sql(drop_stmt)
            col_list = col_list.replace("|",",")
            create_ddl_stage = "create external table if not exists {tbl} ({col_list}) stored as " \
                               "parquet location '{loc}'".format(tbl=db_stg_table_name, col_list=col_list, loc=db_loc)
            insert_stage = "INSERT OVERWRITE TABLE {tbl} select * from tmp_stg_file_ingest".format(
                tbl=db_stg_table_name)
            try:
                hc.sql(create_ddl_stage)
                df_load = df_load.fillna('')
                col_names.insert(len(col_names) - 1, "load_dt")
                df_load = df_load.select(*col_names)
                df_load.registerTempTable("tmp_stg_file_ingest")
                hc.sql(insert_stage)
            except Exception as e:
                log_error(e)
                log_error("====== INSERTION FAILED FOR %s ======" % (db_stg_table_name))
                return_code = 1
                return return_code

            log_info(create_ddl_stage)
            log_info(insert_stage)
            log_info("hdfs_to_stage done")

            hive_count = df_load.count()
            tab_size = run_cmd("hadoop fs -du -h -s " + db_loc)
            bytes_written = tab_size.stdout.split("  ")[0]
            return_code = 0
            aif_meta.target_count = hive_count
            aif_meta.data_size_mb = bytes_written
            aif_meta.status = 'Ref Stage Success'
            # fields = df.schema.fields.show()
        except Exception as e:
            log_error("NO COLUMN DETAILS/ FILE NOT FOUND - EXITING WITHOUT CREATING TABLE")
            log_error(e)
            return_code = 1
    # End of file ingestion to stage

    return return_code


def db_stage_incremental(input_feed, hc, sc, properties, env, aif_meta, col_list):
    """
        Method: db_stage_incremental
        Purpose: To ingest latest records only
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
    log_info("db_stage_incremental")
    if input_feed['ingestion_type'] == 'file-based' and input_feed.get('load_data', 'Y') != 'N':
        db_stg_table_name = "{db}.{tbl}".format(db=input_feed['target_db'], tbl=input_feed['target_table'].lower())
        db_loc = get_table_location(input_feed['target_db'], input_feed['target_table'], properties[env])
        delete_hdfs_directory(db_loc + "/*", True)
        log_info("STG DB PATH %s " % db_loc)
        if input_feed.get('is_timestamp_suffix', 'Y') == 'N':
            delta_time_obj = None
            delta_value=None
        else:
            delta_value =hc.table(input_feed['lake_db'] + "." + input_feed['target_table']).agg(
                {"file_name": "max"}).collect()[0][0]
            delta_value = str(delta_value) if delta_value is not None else delta_value
            if delta_value:
                file_timestamp = delta_value.replace(input_feed['source_table'], '')
                index_end = file_timestamp.rfind('.') if file_timestamp.rfind('.') != -1 else None
                file_timestamp = file_timestamp[:index_end]
                delta_time_obj = datetime.strptime(file_timestamp, input_feed.get('timestamp_format', '%Y%m%d'))
            else:
                delta_time_obj=None
        files_read = get_latest_files(sc, input_feed, delta_time_obj)
        log_info("Max_value of history table " + str(delta_value))
        log_info("Incremental Files read " + str(files_read))
        if not files_read:
            aif_meta.status = 'Skipped stage'
            log_warn('Skipping stage incremental')
            return_code = 1 if files_read is None else 0
            return return_code
        header = True if input_feed['header_avail'].lower().strip() == "true" else False
        select_columns = True if input_feed['select_columns'].lower().strip() == "true" else False
        delimiter = input_feed.get('delimiter', ',')
        col_names = map(lambda x: x.split(" ")[0], col_list.split(","))
        col_names.remove("load_dt")
        schema = generate_schema(col_list)
        df_load = hc.createDataFrame(hc.sparkContext.emptyRDD(), schema)
        try:
            for file in files_read:
                if input_feed.get('is_control_file', 'Y') == 'Y':
                    file = file.replace(input_feed['control_loc'] + "/" + input_feed['control_file'],
                                        input_feed['source_db'] + "/" + input_feed['source_table'])
                if header:
                    special_char = input_feed.get('special_chars_in_column', '')
                    file_encoding_frmt = input_feed.get('file_encoding_frmt', '')
                    header_val = validate_header(sc, col_names, file, delimiter, select_columns, special_char,file_encoding_frmt)
                    if header_val == 1:
                        log_warn('Incorrect header encountered')
                        return header_val

                if input_feed.get('is_schema', 'FALSE') == 'TRUE':
                    df = hc.read.load(file,
                                      format=input_feed['file_format'].lower(),
                                      sep=delimiter, header=header, schema=schema,
                                      timestampFormat="yyyy/MM/dd HH:mm:ss ZZ",
                                      escape=input_feed.get('escape_char', '"'),
                                      quote=input_feed.get('quote_char', '"'),
                                      multiLine=input_feed.get('is_multiline', False))
                else:
                    df = hc.read.load(file,
                                      format=input_feed['file_format'].lower(),
                                      sep=delimiter, header=header,
                                      timestampFormat="yyyy/MM/dd HH:mm:ss ZZ",
                                      escape=input_feed.get('escape_char', '"'),
                                      quote=input_feed.get('quote_char', '"'),
                                      multiLine=input_feed.get('is_multiline', False))

                df = df.withColumn("file_name", lit(file[file.rfind('/') + 1:])) \
                    .select(*col_names)
                df_load = df_load.unionAll(df)
            df_load = df_load.withColumn("load_dt", current_timestamp())
            """Masking Code Starts"""
            if input_feed.get('mask_columns', 0) != 0:
                df_load = generate_sql_masking_df(input_feed, properties[env], hc, sc, df_load)
            """Masking Code ends"""
            # map table with header - column datatype information
            # df_out = column_datatype(self.table_columns,df_load)

            drop_stmt = "drop table if exists {tbl}".format(tbl=db_stg_table_name)
            hc.sql(drop_stmt)
            col_list = col_list.replace("|", ",")
            create_ddl_stage = "create external table if not exists {tbl} ({col_list}) stored as " \
                               "parquet location '{loc}'".format(tbl=db_stg_table_name, col_list=col_list, loc=db_loc)
            insert_stage = "INSERT OVERWRITE TABLE {tbl} select * from tmp_stg_file_ingest".format(
                tbl=db_stg_table_name)
            try:
                hc.sql(create_ddl_stage)
                df_load = df_load.fillna('')
                col_names.insert(len(col_names) - 1, "load_dt")
                df_load = df_load.select(*col_names)
                df_load.registerTempTable("tmp_stg_file_ingest")
                hc.sql(insert_stage)
                log_info(insert_stage)
                log_info("hdfs_to_stage done")
            except Exception as e:
                log_error(e)
                log_error("====== INSERTION FAILED FOR %s ======" % (db_stg_table_name))
                return_code = 1
                return return_code

            hive_count = df_load.count()
            aif_meta.source_count = hive_count
            tab_size = run_cmd("hadoop fs -du -h -s " + db_loc)
            bytes_written = tab_size.stdout.split("  ")[0]
            aif_meta.target_count = hive_count
            aif_meta.data_size_mb = bytes_written
            aif_meta.status = 'Ref Stage Success'
            return_code = 0
            # fields = df.schema.fields.show()
        except Exception as e:
            log_error("NO COLUMN DETAILS AVAILABLE - EXITING WITHOUT CREATING TABLE")
            log_error(e)
            aif_meta.status = 'Skipped stage'
            aif_meta.source_count = 0
            aif_meta.target_count = 0
            log_warn('Skipping stage incremental')
            return_code = 1

    # End of file ingestion to stage
    return return_code


def db_stage_full_selective(input_feed, hc, sc, properties, env, aif_meta):
    pass
