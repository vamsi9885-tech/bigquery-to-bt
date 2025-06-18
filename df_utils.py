#!/bin/python
#####################################################
# File Name: df_utils.py
# Type: Pyspark
# Purpose: Common util methods which are used across all the modules.
# Created: 21/07/2020
# Last Updated: 21/07/2020
# Author: CDP Team
#####################################################
import pyspark
import pyspark.sql.functions as sf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import when
from pyspark.sql.types import MapType, StructType, StructField, StringType, DateType, IntegerType, DoubleType, LongType, \
    DecimalType, FloatType, NullType, TimestampType, ArrayType
from pyspark.sql.window import Window
from pyspark.sql.functions import trim
import datetime
import sys, json
import re
from datetime import *
from subprocess import Popen, PIPE, STDOUT
import os
import time as tm
from src.utils.Storage import AzureStorage

PATTERN_FOR_NULL_CHECK = '[a-zA-Z0-9]+'
LIFE_RISK_CLASS_CODES = ['LIF', 'CIC', 'CIS', 'UWC', 'UWS', 'LAD']
MOTOR_RISK_CLASS_CODES = ['CVN', 'DPC', 'DSC', 'DST', 'DPT']
PATTERN_NON_NUMERIC = "[^0-9\+]"
PROPERTY_RISK_CLASS_CODES = ['STM', 'FLM', 'DVP', 'DCP', 'DGP', 'DPV', 'HGP', 'HCC', 'HVP', 'HPT']


def replace_non_numeric(column_to_replace):
    PHNO_REPLACE_NON_NUMERIC = sf.regexp_replace(column_to_replace, PATTERN_NON_NUMERIC, "")
    return PHNO_REPLACE_NON_NUMERIC


def blank_as_null(df, list_of_colesce_null_columns):
    # list_of_colesce_null_columns = set(list_of_colesce_null_columns)
    # for i in list_of_colesce_null_columns:
    #     df = df.withColumn(i, trim(df[i]))
    df = df.replace("", None)
    return df


def get_latest_record(df, partition_keys, order_keys):
    order_fields = list(map(lambda x: df[x.split(" ")[0]].asc() if x.__contains__("asc") else df[x.split(" ")[0]].desc(),
                       order_keys))
    w = Window.partitionBy(*partition_keys).orderBy(order_fields)
    df = df.withColumn("rnk", sf.rank().over(w)).filter("rnk=1").drop("rnk")
    return df


def get_latest_row(df, partition_keys, order_keys):
    order_keys = list(map(lambda x: sf.col(x).desc(), order_keys))
    w = Window.partitionBy(*partition_keys).orderBy(order_keys)
    df = df.withColumn("rnk", sf.rank().over(w)).filter("rnk=1").drop("rnk")
    return df


def create_sha_field(df, column_name, included_columns=[], num_bits=256):
    filtered_type = df.select(*included_columns).dtypes
    return df.withColumn(column_name, sha2(concat_ws("|", *map(
        lambda col_dtypes: flatten(df[col_dtypes[0]]) if "array<array" in col_dtypes[1] else col_dtypes[0],
        filtered_type)), num_bits))


def cast_cols(df, cols, data_type):
    for i in cols:
        df = df.withColumn(i, sf.col(i).cast(data_type))
    return df


def mart_streamline(df, props):
    MART_DATE_FORMAT = 'MM/dd/yyyy'
    MART_TIMESTAMP_FORMAT = 'MM/dd/yyyy HH:mm:ss'

    if props.get("NUMERIC_COLS"):
        num_cols = props.get("NUMERIC_COLS").split(',')
        for i in num_cols:
            df = df.withColumn(i, when((col(i).isNotNull()) | (col(i) != ''), col(i)).otherwise(lit(0)))
    if props.get("TIMESTAMP_COLS"):
        dt_cols = props.get("TIMESTAMP_COLS").split(',')
        for i in dt_cols:
            df = df.withColumn(i, when((col(i).isNotNull()) | (col(i) != ''),
                                       date_format(to_date(col(i)),
                                                   MART_TIMESTAMP_FORMAT)).otherwise(lit("01/01/1900 00:00:00")))
    if props.get("DATE_COLS"):
        dt_cols = props.get("DATE_COLS").split(',')
        for i in dt_cols:
            df = df.withColumn(i, when((col(i).isNotNull()) | (col(i) != ''),
                                       date_format(to_date(col(i)),
                                                   MART_DATE_FORMAT)).otherwise(lit("01/01/1900")))
    df = df.na.fill('')

    return df


def save_hive(spark, df, columns_obj, hive_db_name, hive_table_name, mode="append", partition_column="source,active_ym",
              repartition=None, app_logger=None):
    """
    :param spark:
    :type spark:
    :param df:
    :type df:
    :param columns_obj:
    :type columns_obj:
    :param hive_db_name:
    :type hive_db_name:
    :param hive_table_name:
    :type hive_table_name:
    :param partition_column:
    :type partition_column:
    """
    try:
        df = df.select(*columns_obj['TARGET'].split(","))
        df = df.dropDuplicates()
        df.persist(pyspark.StorageLevel(True, True, False, False, 1))

        write_to_hive = {
            "append": df.write.mode("append").format("hive"),
            "append_parquet": df.write.mode("append").format("parquet"),
            "append_partition": df.write.format("parquet").partitionBy(*partition_column.split(",")).mode("append")
        }
        write_to_hive[mode].saveAsTable(hive_db_name + "." + hive_table_name)
    except Exception as e:
        df.unpersist()
        spark.catalog.refreshTable(hive_db_name + "." + hive_table_name)
        app_logger.error(__name__, str(e))
        sys.exit(1)
    finally:
        df.unpersist()
        spark.catalog.refreshTable(hive_db_name + "." + hive_table_name)


def save_hive_overwrite(spark, df, columns_obj, hive_db_name, hive_table_name, mode="overwrite",
                        partition_column="source,active_ym", app_logger=None):
    """
    :param spark:
    :type spark:
    :param df:
    :type df:
    :param columns_obj:
    :type columns_obj:
    :param hive_db_name:
    :type hive_db_name:
    :param hive_table_name:
    :type hive_table_name:
    :param partition_column:
    :type partition_column:
    """
    try:
        df = df.select(*columns_obj['TARGET'].split(","))
        df = df.dropDuplicates()
        df.persist(pyspark.StorageLevel(True, True, False, False, 1))
        write_to_hive = {
            "overwrite": df.write.mode("overwrite").format("parquet").option("path", columns_obj.get('TARGET_DB_PATH',
                                                                                                     '') + hive_table_name),
            "overwrite_partition": df.write.partitionBy(*partition_column.split(",")).mode("overwrite").option("path",
                                                                                                               columns_obj.get(
                                                                                                                   'TARGET_DB_PATH',
                                                                                                                   '') + hive_table_name).format(
                "parquet")
        }
        write_to_hive[mode].saveAsTable(hive_db_name + "." + hive_table_name)
    except Exception as e:
        df.unpersist()
        app_logger.error(__name__, str(e))
        sys.exit(1)
    finally:
        df.unpersist()
        spark.catalog.refreshTable(hive_db_name + "." + hive_table_name)


def read_json(json_file):
    with open(json_file) as json_file:
        json_obj = json.load(json_file)
    return json_obj


def regex_check(element, json_obj):
    pattern = json_obj['value']
    if re.match(pattern, element):
        return True
    else:
        return False


def nregex_check(element, json_obj):
    pattern = json_obj['value']
    if re.match(pattern, element):
        return False
    else:
        return True


# Method applies rules over the Date columns

def nregex_find_check(element, json_obj):
    pattern = json_obj['value']
    if re.findall(pattern, element):
        return False
    else:
        return True


def dob_operators(splitted_date, matcher, operator):
    """Condition for year in list"""
    date_ruler = {
        "gt": lambda splitted_date, matcher: True if int(splitted_date) > int(matcher) else False,
        'lt': lambda splitted_date, matcher: True if int(splitted_date) < int(matcher) else False,
        'eq': lambda splitted_date, matcher: True if int(splitted_date) == int(matcher) else False,
        'ne': lambda splitted_date, matcher: True if int(splitted_date) != int(matcher) else False
    }
    date_ruler_func = date_ruler.get(operator)
    date_ruler_string = date_ruler_func(splitted_date, matcher)
    return date_ruler_string


# Function which validates the Dates


def dob_check(element, json_obj):
    date_string = str(element)
    try:
        date_format = json_obj['format']
        datetimeobject = datetime.strptime(date_string, date_format)
        date_rules_obj = json_obj['date_rules']
        dob_result_list = list()
        for i in date_rules_obj.keys():
            date_rule_obj = date_rules_obj[i]
            matcher = str(date_rule_obj['value'])

            operator = str(date_rule_obj['operator'])

            """Determining on which category the rule to be applied (It may be year ,month,Day)"""
            splitted_date_dict = {
                'year': datetimeobject.year,
                'month': datetimeobject.month,
                'day': datetimeobject.day
            }
            splitted_date = splitted_date_dict[date_rule_obj['validity_check']]
            dob_result_list.append(dob_operators(splitted_date, matcher, operator))
        if False in dob_result_list:
            dob_result = False
        else:
            dob_result = True
    except ValueError as e:

        return False
    except Exception as e:
        return False
    return dob_result


def list_check(element, json_obj):
    if element in json_obj['value']:
        return True
    else:
        return False


def not_in_list_check(element, json_obj):
    if element not in json_obj['value']:
        return True
    else:
        return False


def string_check(element, json_obj):
    element_string = str(element)
    if element_string == json_obj['value']:
        return True
    else:
        return False


def getConfig(sparkContext):
    conf_obj = sparkContext.getConf()
    return conf_obj


def add_unique_id(df, pk_column, pk_initial):
    """
    Adds a unique ID column to a DataFrame.

    Args:
        df: The input DataFrame.
        pk_column: The name of the new primary key column.
        pk_initial: The initial value for the primary key.

    Returns:
        A DataFrame with an additional unique ID column.
    """
    res_df_schema = StructType(df.schema.fields + [StructField(pk_column, IntegerType(), False)])
    return df.rdd.zipWithIndex() \
        .map(lambda row_id: {**row_id[0].asDict(), pk_column: row_id[1] + pk_initial}) \
        .toDF(res_df_schema)


class Result:
    pass


def run_cmd(cmd, app_logger):
    p = Popen(cmd, shell=True, stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds=True, universal_newlines=True,
              bufsize=-1, env=dict(os.environ))
    (stdout, stderr) = p.communicate()
    app_logger.info(__name__, "Executing the command " + str(cmd))
    if str(stderr) is not None:
        app_logger.error(__name__, "stderr is " + str(stderr))
    return p.returncode


def delete_hdfs_directory(path, app_logger, skipTrash=False):
    if skipTrash:
        run_cmd("hadoop fs -rm -R -f -skipTrash " + path, app_logger)
    else:
        run_cmd("hadoop fs -rm -R -f " + path, app_logger)


def delete_path(sc, path):
    try:
        storage = AzureStorage.get_storage_instance()
        fs = storage.hadoopfs
        fs.delete(storage.hadoopjvm.fs.Path(path), True)
        print("Deleted the HDFS directory " + path)
    except:
        print("Error deleting HDFS path " + path)



def selective_df(spark, db_table, column_list, start_date=None, end_date=None, delta_date_format=None):
    """
    :param spark: Spark session object
    :param db_table: Table from which data is to be read
    :param column_list: List of columns to be select from the entire table
    :param start_date: start date of window
    :param end_date: End date of window
    :return: Data frame with selected columns and filter condition applied.
    """
    if isinstance(column_list, list):
        df = spark.read.table(db_table)
        return df.select(*column_list)
    elif isinstance(column_list, str) and column_list.upper() == "ALL":
        return spark.read.table(db_table)
    elif isinstance(column_list, dict):
        rnk_fields = column_list.get('rnk_fields')
        sel_fields = column_list['sel_fields'].split(",")
        df = spark.read.table(db_table).select(*sel_fields)
        if column_list.get('FILTER_CONDITION', 0):
            df = df.filter(column_list['FILTER_CONDITION'])
        if start_date is not None:
            delta_column = column_list.get('delta_column', '').strip()
            if delta_date_format is not None:
                filter_cond = "((to_timestamp(" + delta_column + ",'" + delta_date_format + "') >= to_timestamp('" + start_date + "','" + delta_date_format + "')) and   (to_timestamp(" + delta_column + ",'" + delta_date_format + "') < to_timestamp('" + end_date + "','" + delta_date_format + "') ))"
                df = df.filter(filter_cond)
            else:
                df = df.filter((df[delta_column] >= lit(start_date)) & (df[delta_column] < lit(end_date)))
        if start_date is None and end_date is not None:
            delta_column = column_list.get('delta_column', '').strip()
            df = df.filter(df[delta_column] < lit(end_date))
        if rnk_fields is not None:
            part_fields = column_list.get('part_fields')
            part_concat_fields = column_list.get('part_concat_fields')
            rnk_fields = rnk_fields.split(",")
            order_fields = list(map(
                lambda x: df[x.split(" ")[0]].asc() if x.__contains__("asc") else df[x.split(" ")[0]].desc(),
                rnk_fields))
            if part_fields is not None:
                w = Window.partitionBy(*part_fields.split(",")).orderBy(order_fields)
            else:
                w = Window.partitionBy(sf.concat_ws('', *part_concat_fields.split(","))).orderBy(order_fields)
            if column_list.get('row_num') is not None:
                df = df.withColumn("rornk", sf.row_number().over(w)).filter("rornk=1").drop("rornk")
            else:
                df = df.withColumn("rnk", sf.rank().over(w)).filter("rnk=1").drop("rnk")
            # Drop duplicates to remove exact duplicates
            df = df.dropDuplicates()
        return df

def filter_blacklist_ip(spark, df, src_tbl, blacklist_key, blacklist_ips):
    w = Window.partitionBy(col("quote_key")).orderBy(
        col("rec_version").desc())
    blacklist_ips = blacklist_ips.split(",")
    quote_df = spark.read.table(src_tbl[0] + "_gi_hub_cdp.mstr_quote")

    quote_df = quote_df.withColumn("quote_key", concat(col("quote_number"), col("quote_folder_id"))).withColumn("rnk",
                                                                                                                rank().over(
                                                                                                                    w)).filter(
        (col("rnk") == 1) & (col("source") == "SAF2_QUOTE")).select(
        col("quote_key"), col("quote_originating_ip"), col("agent_intermediary_id")).dropDuplicates()

    if "," in blacklist_key:
        df = df.withColumn("quote_key", concat(("quote_number"), col("quote_folder_id")))
        blacklist_key = "quote_key"

    joined_df = df.alias("n").join(quote_df.alias("h"),
                                   col("h.quote_key") == col("n." + blacklist_key),
                                   how="left_outer")
    df1 = joined_df.filter(col("h.quote_key").isNull()).select("n.*")

    df2 = joined_df.filter((col("h.quote_key").isNotNull())).filter(
        ((~col("h.quote_originating_ip").isin(blacklist_ips)) |
         (col("h.quote_originating_ip").isNull())) & (
                (col("h.agent_intermediary_id") != "SAF2TEST") | col("h.agent_intermediary_id").isNull())).select("n.*")

    df = df1.union(df2)
    return df


def selective_df_mart(spark, column_list, src_tbl, target_tbl, blacklist_ips=None, delta_column="load_dt"):
    if isinstance(column_list, list):
        filter_condition = None
        sel_fields = column_list
        rnk_fields = None
        blacklist_key = None
    else:
        filter_condition = column_list.get('FILTER_CONDITION')
        rnk_fields = column_list.get('rnk_fields')
        sel_fields = column_list['sel_fields'].split(",")
        blacklist_key = column_list.get('blacklist_key')

    df = spark.read.table(src_tbl).select(*sel_fields)
    if "policy" in src_tbl.lower():
        source_table_policy = src_tbl[0] + "_gi_hub_cdp.mstr_policy"
        # Filter condition to filter those policies where policy end date is less than 2014-12-31 23:59:59
        w = Window.partitionBy(sf.col("policy_key")) \
            .orderBy(sf.col("rec_version").desc(), sf.col("active_dt").desc())
        filter_cond = "from_unixtime(unix_timestamp(policy_end_date, 'yyyyMMdd'),'yyyy-MM-dd HH:mm:ss') >= '2015-01-01 00:00:00'"
        filter_df = spark.read.table(source_table_policy).withColumn("rnk", sf.rank().over(w)).filter("rnk = 1").drop(
            "rnk")
        filter_df = filter_df.filter(filter_cond)
        if "policy_key" not in df.columns:
            filter_df = filter_df.withColumn("key_column", sf.col("client_key"))
            df = df.alias("n").join(filter_df.alias("h"), sf.col("n.client_key") == sf.col("h.key_column"), "inner") \
                .select("n.*")
        else:
            filter_df = filter_df.withColumn("key_column", sf.col("policy_key"))
            df = df.alias("n").join(filter_df.alias("h"), sf.col("n.policy_key") == sf.col("h.key_column"), "inner") \
                .select("n.*")
    if "quote" in src_tbl.lower():
        source_table_quote = src_tbl[0] + "_gi_hub_cdp.mstr_quote"
        filter_cond = "months_between(current_timestamp(),(coalesce(quote_prepared_date,quote_created_ts))) <= 12.00"
        filter_df = spark.read.table(source_table_quote).filter(filter_cond)
        w = Window.partitionBy("key_column") \
            .orderBy(sf.col("rec_version").desc(), sf.col("active_dt").desc())
        filter_df = filter_df.withColumn("key_column",
                                         concat_ws('', sf.col("quote_number"), sf.col("quote_folder_id")))
        if spark.read.table(target_tbl).agg(max(sf.col(delta_column))).collect()[0][0] == None:
            filter_df = filter_df.withColumn("rnk", sf.rank().over(w)).filter("rnk = 1").drop("rnk")
            filter_df = filter_df.filter(~((filter_df["quote_contract_stage"].isin("NB", "NewBusiness")) & (
                filter_df["quote_contract_status"].isin("CL", "Closed")) & (
                                               filter_df["quote_processing_status"].isin("Complete", "C")) & (
                                                   (filter_df["policy_number"].isNotNull()) | (
                                                   filter_df["policy_number"] != ''))))
        if "client_key" in df.columns:
            df = df.alias("n").join(filter_df.alias("h"), sf.col("n.client_key") == sf.col("h.key_column"),
                                    "inner").select("n.*")
        elif "quote_key" not in df.columns:
            df = df.alias("n").join(filter_df.alias("h"),
                                    concat_ws('', sf.col("n.quote_number"), sf.col("n.quote_folder_id")) == sf.col(
                                        "h.key_column"), "inner").select("n.*")
        else:
            df = df.alias("n").join(filter_df.alias("h"), sf.col("n.quote_key") == sf.col("h.key_column"),
                                    "inner").select("n.*")
    if filter_condition is not None:
        df = df.filter(filter_condition)
    if blacklist_key is not None:
        df = filter_blacklist_ip(spark, df, src_tbl, blacklist_key, blacklist_ips)
    if rnk_fields is not None:
        part_fields = column_list.get('part_fields')
        part_concat_fields = column_list.get('part_concat_fields')
        rnk_fields = rnk_fields.split(",")
        order_fields = list(map(
            lambda x: df[x.split(" ")[0]].asc() if x.__contains__("asc") else df[x.split(" ")[0]].desc(),
            rnk_fields))
        if part_fields is not None:
            w = Window.partitionBy(*part_fields.split(",")).orderBy(order_fields)
        else:
            w = Window.partitionBy(sf.concat_ws('', *part_concat_fields.split(","))).orderBy(order_fields)
        if column_list.get('row_num') is not None:
            df = df.withColumn("rornk", sf.row_number().over(w)).filter("rornk=1").drop("rornk")
        else:
            df = df.withColumn("rnk", sf.rank().over(w)).filter("rnk=1").drop("rnk")
    return df


# The following functions are used in the client and agent domain

def rename_columns(df, renaming_dict):
    renaming_dict = dict(renaming_dict)
    for i in renaming_dict.keys():
        df = df.withColumnRenamed(i, renaming_dict.get(i))
    return df


def copy_columns(df, copy_dict):
    copy_dict = dict(copy_dict)
    for i in copy_dict.keys():
        df = df.withColumn(i, sf.col(copy_dict.get(i)))
    return df


def adding_hardcoded_columns(df, list_of_hardcoded_columns, value=None, col_type=StringType()):
    if isinstance(list_of_hardcoded_columns, dict):
        for i in list_of_hardcoded_columns.keys():
            if list_of_hardcoded_columns[i] == ArrayType(StringType()):
                df = df.withColumn(i, array(sf.lit(value).cast(StringType())))
            else:
                df = df.withColumn(i, sf.lit(value).cast(list_of_hardcoded_columns[i]))
    else:
        if col_type == ArrayType(StringType()):
            for i in list_of_hardcoded_columns:
                df = df.withColumn(i, array(sf.lit(value).cast(StringType())))
        elif col_type == "empty_array":
            for i in list_of_hardcoded_columns:
                df = df.withColumn(i, array())
        else:
            for i in list_of_hardcoded_columns:
                df = df.withColumn(i, sf.lit(value).cast(col_type))
    return df


def read_hive_table(spark, db_name, tbl_name, columns):
    df = spark.table(db_name + "." + tbl_name).select(*columns).dropDuplicates()
    return df


def when_value(df, when_col, value, list_of_columns, hardcoded_value):
    for i, j in list_of_columns:
        df = df.withColumn(i, when(sf.col(when_col) == value, sf.col(j)).otherwise(sf.lit(hardcoded_value)))
    return df


def null_check_columns(df, list_of_null_check_columns, check_column, if_satisfy, else_satisfy=""):
    for i in list_of_null_check_columns:
        df = df.withColumn(i, when(sf.col(check_column).rlike("[^()]"), sf.col(if_satisfy)).otherwise(
            sf.lit(else_satisfy)))
    return df


def schema_seperator(df, seperator):
    for i in df.columns:
        df = df.withColumnRenamed(i, str(i) + "_" + str(seperator))
    return df


def build_lookup_table(spark, select_columns, source_db, source_schema, source_table, conditions):
    select_items = ""
    for i in select_columns:
        select_items += "trim(" + i + "), "
    select_items = select_items[:-2]
    filter_condition = ""
    for i in conditions.keys():
        filter_condition += conditions[i] + " and "
    filter_condition = filter_condition[:-5]
    sql_string = "select distinct " + select_items + " from " + source_db + "." + source_schema + "_" + source_table \
                 + " where " + filter_condition + ""
    df = spark.sql(sql_string)
    lookup_table = df.rdd.map(lambda x: (x[0], x[1])).collectAsMap()
    return lookup_table


def merge_two_dicts(x, y):
    """
    :param x: First dictionary
    :param y: Second dictionary
    :return: merged dictionary
    """
    result = x.copy()  # start with x's keys and values
    result.update(y)  # modifies z with y's keys and values & returns None
    return result


def read_surr_table(sc, table_name, source):
    """
    :param sc: Spark context
    :param table_name: pass the table surrogate key table name to be read
    :param source: pass the data domain for which table is to read
    :return: Data frame with the natural key and surrogate key
    """
    df = sc.sql("select surr_key, natural_key from {tbl} where domain = '{domain_name}'".format(tbl=table_name,
                                                                                                domain_name=source))
    return df


def date_time_now():
    return datetime.now()


def get_year_month(str_dt, in_dt_frmt='%Y-%m-%d %H:%M:%S.%f', out_dt_frmt='%Y-%m'):
    dt = datetime.strptime(str_dt, in_dt_frmt)
    if out_dt_frmt == '%Y%m':
        return str(dt.year) + str(dt.month)
    elif out_dt_frmt == '%Y-%m':
        return str(dt.year) + '-' + str(dt.month)


def filter_non_printable_chars(df, col_list):
    """
    :param df: Initial data frame.
    :param col_list: List of columns on which this filter has to be applied.
    :return: The filtered data frame.
    """
    filtered_chars = '\x00|\x01|\x02|\x03|\x04|\x05|\x06|\x07|\x08|\t|\n|\x0b|\x0c|\r|\x0e|\x0f|\x10|\x11|\x12|\x13|\x14|\x15|\x16|\x17|\x18|\x19|\x1a|\x1b|\x1c|\x1d|\x1e|\x1f|\x7f'
    filtered_chars = '\x00,\x01,\x02,\x03,\x04,\x05,\x06,\x07,\x08,\t,\n,\x0b,\x0c,\r,\x0e,\x0f,\x10,\x11,\x12,\x13,\x14,\x15,\x16,\x17,\x18,\x19,\x1a,\x1b,\x1c,\x1d,\x1e,\x1f,\x7f,""'
    filtered_chars = filtered_chars.split(",")
    for i in col_list:
        # df = df.filter(~df[i].isin(filtered_chars)).filter(~trim(df[i]).isin(''))
        df = df.filter(~trim(df[i]).isin(filtered_chars))
    return df


def add_audit_columns(df, schema, excluded_columns, source_dt_format="yyyy-MM-dd"):
    sel_list = [x for x in df.columns if x not in excluded_columns]
    df = create_sha_field(df, "rec_sha", sel_list)
    if schema:
        df = df.withColumn("source", lit(schema))
    df = df.withColumn("active_ym", date_format(to_date(df.active_dt, source_dt_format), "yyyy-MM"))
    time_now = date_time_now()
    df = df.withColumn("load_dt", lit(time_now))
    return df


def add_audit_columns_mart(df, props, excluded_columns=[], source_dt_format="yyyy-MM-dd"):
    sel_list = [x for x in df.columns if x not in excluded_columns]
    df = mart_streamline(df, props)
    df = create_sha_field(df, "rec_sha", sel_list)
    time_now = date_time_now()
    df = df.withColumn("load_dt", lit(time_now))
    df = df.withColumn("active_ym", date_format(to_date(df.load_dt, source_dt_format), "yyyy-MM"))
    return df


def merge_two_dfs(spark, props, schemas, table_name, select_list):
    for num, schema in enumerate(schemas.split(",")):
        schema = schema.strip()
        temp_df = selective_df(spark, props["SOURCE_DB_LAK_DWH"] + "." + schema + "_" + table_name,
                               select_list, props['start_date'], props['end_date']).withColumn("source_table",
                                                                                               lit(str(schema + '_' +
                                                                                                       table_name).upper()))
        if (num == 0):
            df = temp_df
        else:
            df = df.union(temp_df)
    return df

def merge_two_dfs_premium(spark, props, schemas, table_name, select_list):
    for num, schema in enumerate(schemas.split(",")):
        schema = schema.strip()
        temp_df = selective_df_premium(spark, props["SOURCE_DB_LAK_DWH"] + "." + schema + "_" + table_name,
                               select_list, props['start_date'], props['end_date']).withColumn("source_table",
                                                                                               lit(str(schema + '_' +
                                                                                                       table_name).upper()))
        if (num == 0):
            df = temp_df
        else:
            df = df.union(temp_df)
    return df

def get_explode_df(df, zip_columns, audit_columns):
    """
    This method would zip the columns, explode it all together and convert single array row into multiple rows for
    array columns and repeat the remaining columns
    :param df: The df in which columns needs to be zipped
    :param zip_columns: The columns that needs to be zipped
    :param tbl_select_columns: The columns list that needs to be selected after explode
    :param audit_columns: The record keeping columns described above
    :return: df
    """
    # df = df.select(*tbl_select_columns)
    df = df.withColumn("tmp", arrays_zip(*zip_columns)).withColumn("tmp", explode("tmp"))
    zip_columns = ["tmp." + x for x in zip_columns]
    select_columns = audit_columns + zip_columns
    df = df.select(*select_columns)
    return df


def get_transformation_riskcover(base_df, union_list, sel_list, json_obj):
    for num, i in enumerate(sel_list.split(",")):
        sel_fields = union_list[i].split(",")
        temp_df = rename_columns(base_df.select(*sel_fields), json_obj.get(i))
        if num == 0:
            df = temp_df
        else:
            df = df.union(temp_df)
    return df


def cast_columns(df, cast_dict, input_date_format="yyyy-MM-dd", output_date_format="yyyy-MM-dd"):
    """
    This function will cast the each colmn of the given data frame which is referred in the cast_dict.
    The desired output format is mentioned as a value of the dictionary key and the dictionary key is the column name.
    :param df: Inout df
    :param cast_dict: The dictionary which maintain the column name and its desired column type
    :param input_date_format: Optional. If any date column then what is the input format and what is output format
    :param output_date_format: Optional. If any date column then what is the input format and what is output format
    :return: The data frame with casted columns
    """
    for i in cast_dict.keys():
        if cast_dict.get(i) == "timestamp":
            df = df.withColumn(i, from_unixtime(unix_timestamp(i, input_date_format), output_date_format).cast(
                TimestampType()))
        df = df.withColumn(i, df[i].cast(cast_dict.get(i)))
    return df


def get_active_date(df, active_dt_column, input_date_format="yyyyMMDD", output_date_format="yyyy-MM-dd"):
    df = df.withColumn("active_dt",
                       from_unixtime(unix_timestamp(df[active_dt_column], input_date_format), output_date_format).cast(
                           TimestampType()))
    df = df.withColumn("active_ym", date_format(to_date(col("active_dt"), "yyyy-MM-dd"), "yyyy-MM"))
    return df


def jtodate_udf(derive_from, date_eff):
    try:
        string = datetime.strptime(str(derive_from).split()[0], '%Y%j').strftime('%Y-%m-%d')
    except Exception as Error:
        string = datetime.strptime(str(date_eff).split()[0], '%Y%m%d').strftime('%Y-%m-%d')
    return string


def dttodate_udf(derive_from):
    try:
        string = datetime.strptime(str(derive_from).split()[0], '%Y%m%d').strftime('%Y-%m-%d' ' ' + '00:00:00.0')
    except Exception as Error:
        string = ''
    return string


def add_default_values(df, default_dict):
    for i in default_dict.keys():
        if default_dict.get(i) == "None":
            df = df.withColumn(i, lit(None).cast(StringType()))
        else:
            val = default_dict.get(i).split(",")
            df = df.withColumn(i, lit(val[0]).cast(val[1]))
    return df

def convert_list_to_dict(lst):
    res_dct = {lst[i][1]: str(lst[i][0]) for i in range(0, len(lst))}
    return res_dct

def convert_date_adobe(df, col_list, source_format="yyyy-MM-dd HH:mm:ss", target_format="MM/dd/yyyy HH:mm:ss"):
    col_list = dict(col_list)
    for i in col_list.keys():
        df = df.withColumn(i, from_unixtime(unix_timestamp(col_list.get(i), source_format),
                                            target_format))
    return df


def filternullempty(df, col_list):
    for i in col_list:
        df = df.filter((col(i).isNotNull()) & (col(i) != ""))
    return df


def add_default_cols(df, col_list):
    col_list = dict(col_list)
    for i in col_list.keys():
        df = df.withColumn(i, sf.lit(col_list.get(i)))
    return df


def time_udf(derive_from):
    try:
        result = tm.strftime("%I:%M:%S", tm.strptime(derive_from, "%H%M%S"))
    except Exception as error:
        stage = str(derive_from[:4]) + "59"
        result = tm.strftime("%I:%M:%S", tm.strptime(str(stage), "%H%M%S"))
    return result


def filter_incorrect_source_records(df):
    """
    This method will arrange the source table column for all the risk data domains
    :param df: The original dataframe.
    :return:
    """
    w_rec_sha = Window.partitionBy("rec_sha").orderBy(sf.col("active_dt").desc())
    df_c = df.filter(
        "(risk_key like '6%' and source_table like 'DIRECT%') or (risk_key like '1%' and source_table like 'DBADM%')")
    df_i = df.filter(
        "(risk_key like '6%' and source_table like 'DBADM%') or (risk_key like '1%' and source_table like 'DIRECT%')")
    df_c = df_c.withColumn("rec_sha_rnk", sf.dense_rank().over(w_rec_sha)).filter("rec_sha_rnk = 1")
    df_i = df_i.withColumn("rec_sha_rnk", sf.dense_rank().over(w_rec_sha)).filter("rec_sha_rnk = 1")
    df_i = df_i.alias("n").join(df_c.alias("h"), sf.col("n.rec_sha") == sf.col("h.rec_sha"), how='left_anti') \
        .select([sf.col('n.' + c) for c in df_c.columns])
    df = df_c.union(df_i)
    return df


def trim_cols(df, col_list):
    for i in col_list:
        df = df.withColumn(i, sf.trim(sf.col(i)))
    return df

def cast_cols_to_timestamp(df, cols, data_type, source_format,target_format):
    for i in cols:
        df = df.withColumn(i, from_unixtime(unix_timestamp(sf.col(i), source_format),
                                            target_format).cast(data_type))
    return df

def cast_cols_decimal(df, cols, data_type):
    for i in cols:
        df = df.withColumn(i, when((sf.col(i).isNotNull()) | (sf.col(i) != ''), sf.col(i).cast(data_type)).otherwise(lit(0.00000)))
    return df

'''Adding columns to dataframe so as to 'normalize'/ make consistent between dfs'''
def normalisation(target_columns, df):
    none_value_cols = list(set(target_columns) - set(df.columns))
    if none_value_cols:
        df = adding_hardcoded_columns(df, none_value_cols)
    return df.select(*target_columns)

def delete_hdfs_path(spark, table_loc):
    storage = AzureStorage.get_storage_instance()
    fs = storage.hadoopfs
    conf = storage.hadoop_conf
    path = storage.hadoopjvm.fs.Path(table_loc)
    list(map(lambda x: fs.get(conf).delete(x.getPath()), fs.get(conf).globStatus(path)))


def selective_df_premium(spark, db_table, column_list, start_date=None, end_date=None):
    """
    :param spark: Spark session object
    :param db_table: Table from which data is to be read
    :param column_list: List of columns to be select from the entire table
    :param start_date: start date of window
    :param end_date: End date of window
    :return: Data frame with selected columns and filter condition applied.
    """
    if isinstance(column_list, list):
        df = spark.read.table(db_table)
        return df.select(*column_list)
    elif isinstance(column_list, str) and column_list.upper() == "ALL":
        return spark.read.table(db_table)
    elif isinstance(column_list, dict):
        rnk_fields = column_list.get('rnk_fields')
        sel_fields = column_list['sel_fields'].split(",")
        df = spark.read.table(db_table).select(*sel_fields)
        active_julian_date = column_list.get('active_julian_date', None)
        if active_julian_date is not None:
            df = df.withColumn("aud_transaction_date",
                               substring(from_unixtime(unix_timestamp(col(column_list.get('active_julian_date')),
                                                                      'yyyyDDD')), 1, 10)) \
                .withColumn("aud_transaction_time",
                            when((regexp_extract(col(column_list.get('active_time')), '(\\d{2})(\\d{2})(\\d{2})', 3) == 99),
                                 concat_ws("", regexp_extract(col(column_list.get('active_time')),
                                                              '(\\d{2})(\\d{2})(\\d{2})', 1), lit(":"),
                                           regexp_extract(col(column_list.get('active_time')),
                                                          '(\\d{2})(\\d{2})(\\d{2})', 2), lit(":59"))).otherwise(
                                concat_ws("", regexp_extract(col(column_list.get('active_time')),
                                                             '(\\d{2})(\\d{2})(\\d{2})', 1), lit(":"),
                                          regexp_extract(col(column_list.get('active_time')),
                                                         '(\\d{2})(\\d{2})(\\d{2})', 2), lit(":"),
                                          regexp_extract(col(column_list.get('active_time')),
                                                         '(\\d{2})(\\d{2})(\\d{2})', 3))))
            df = df.withColumn("active_dt",
                               concat_ws(" ", col("aud_transaction_date"),
                                         col("aud_transaction_time")).cast(TimestampType()))
        if column_list.get('FILTER_CONDITION', 0):
            df = df.filter(column_list['FILTER_CONDITION'])
        if start_date is not None:
            delta_column = column_list.get('delta_column', '').strip()
            df = df.filter((df[delta_column] >= lit(start_date)) & (df[delta_column] < lit(end_date)))
        if start_date is None and end_date is not None:
            delta_column = column_list.get('delta_column', '').strip()
            df = df.filter(df[delta_column] < lit(endf_date))
        if rnk_fields is not None:
            part_fields = column_list.get('part_fields')
            part_concat_fields = column_list.get('part_concat_fields')
            rnk_fields = rnk_fields.split(",")
            order_fields = list(map(
                lambda x: df[x.split(" ")[0]].asc() if x.__contains__("asc") else df[x.split(" ")[0]].desc(),
                rnk_fields))
            if part_fields is not None:
                w = Window.partitionBy(*part_fields.split(",")).orderBy(order_fields)
            else:
                w = Window.partitionBy(sf.concat_ws('', *part_concat_fields.split(","))).orderBy(order_fields)
            if column_list.get('row_num') is not None:
                df = df.withColumn("rornk", sf.row_number().over(w)).filter("rornk=1").drop("rornk")
            else:
                df = df.withColumn("rnk", sf.rank().over(w)).filter("rnk=1").drop("rnk")
            # Drop duplicates to remove exact duplicates
            df = df.dropDuplicates()
        return df


def get_partition(df, partition_deriving_columns, partition_logic, partition_transform_columns):
    if partition_logic is not None:
        derive_from = partition_deriving_columns.split(",")
        partition_logic = partition_logic.split(",")
        partition_transform = partition_transform_columns.split(",")
        x = 0
        for i in partition_transform:
            if i != '':
                format_from, format_to = partition_logic[x].split('|')
                df = df.withColumn(i, date_format(to_date(from_unixtime(unix_timestamp(sf.col(derive_from[x]), format_from), "yyyy-MM-dd").cast(
                           TimestampType()), "yyyy-MM-dd"), format_to ))
            x = x + 1
    return df


def get_active_date_premium(df,columns_list):
    ### function to convert Julian date to normal calender date for premium DB load
    df = df.withColumn("aud_transaction_date",
                       substring(from_unixtime(unix_timestamp(col(columns_list[0]),'yyyyDDD')), 1, 10)) \
        .withColumn("aud_transaction_time",
                    when((regexp_extract(col(columns_list[1]), '(\\d{2})(\\d{2})(\\d{2})', 3) == 99),
                         concat_ws("", regexp_extract(col(columns_list[1]),
                                                      '(\\d{2})(\\d{2})(\\d{2})', 1), lit(":"),
                                   regexp_extract(col(columns_list[1]),
                                                  '(\\d{2})(\\d{2})(\\d{2})', 2), lit(":59"))).otherwise(
                        concat_ws("", regexp_extract(col(columns_list[1]),
                                                     '(\\d{2})(\\d{2})(\\d{2})', 1), lit(":"),
                                  regexp_extract(col(columns_list[1]),
                                                 '(\\d{2})(\\d{2})(\\d{2})', 2), lit(":"),
                                  regexp_extract(col(columns_list[1]),
                                                 '(\\d{2})(\\d{2})(\\d{2})', 3))))
    df = df.withColumn("active_dt", concat_ws(" ", col("aud_transaction_date"),col("aud_transaction_time")).cast(TimestampType()))\
              .drop("aud_transaction_date","aud_transaction_time")
    return df

def split_str_to_cols(df,split_column,columns):
    """
        Method uses to split string column delimited by comma into multiple columns
        :param df:
        :param split_column
        :param columns: list of splitted columns
    """
    for column in columns:
        df = df.withColumn(str(column), split(col(split_column), ',').getItem(columns.index(column)))
    return df

def get_hadoop_fs_and_path(spark):
    storage = AzureStorage.get_storage_instance()
    fs = storage.hadoopfs
    Path = storage.hadoopjvm.fs.Path
    return fs, Path

def rename_hdfs_file(fs, Path, folder, app_logger, new_path):
    """
    Method is used to rename the hdfs part file inside a folder
     :param fs
     :param Path
     :param folder: folder path
     :param app_logger: logger name
     :param file_name : new file_name
    """
    try:
        file_status = fs.listStatus(Path(folder))
        for file in file_status:
            if str(file.getPath().getName()).startswith("part-"):
                print(file.getPath())
                status = fs.rename(file.getPath(), Path(new_path))
                if not status:
                    raise
    except Exception as ex:
        app_logger.error(__name__, str(ex))
        raise

def enrich_data(df, entity_details):
    """
    Method adds enriched columns
    :param df:
    :param entity:
    :param entity_details:
    :return: Dataframe
    """
    df = df.withColumn("external_entity_name", lit(str(entity_details.get("external_entity_name"))))\
           .withColumn("external_entity_type",lit(str(entity_details.get("external_entity_type"))))\
           .withColumn("business_unit",lit(str(entity_details.get("business_unit"))))
    return df

def add_audit_columns_complaints(df, entity, included_columns, active_column="active_ym"):
    """
    Method adds audit columns
    :param df:
    :param entity:
    :param rec_sha:
    :param included_columns:
    :param source_dt_format:
    :return: Dataframe
    """
    active_col= {"active_ym":date_format(col('active_dt'),'yyyy-MM'),
                 "active_yr":year('load_dt')}
    if included_columns:
        df = create_sha_field(df, "rec_sha", included_columns)
    df = df.withColumnRenamed('load_dt','active_dt')
    df = df.withColumn("entity", lit(entity))\
           .withColumn("load_dt", current_timestamp())\
           .withColumn(active_column, active_col.get(active_column))
    return df

def parse_json_object(df, config):
    """
    Method to extract specific data from json string
    :param df: input dataframe
    :param config: input configuration to extract json data
        Sample Config:
        "parse_json_object": [
			{
				"source_column": "<input column name>",
				"target_column": "<target column name>",
				"json_path": "$.<child>.<sub-child>"
			}
		]
    :return: Dataframe
    """
    for parser_config in config:
        source_column = parser_config.get("source_column")
        target_column = parser_config.get("target_column")
        json_path = parser_config.get("json_path")
        df = df.withColumn(target_column, get_json_object(col(source_column),json_path))
    return df
    
def create_sha(df, excluded_cols=[]):
    """
    This method takes the input as a spark dataframe and columns to be skipped from the dataframe .
    """
    sel_list = [x for x in df.columns if x not in excluded_cols]
    return create_sha_field(df, 'rec_sha', sel_list)

def selective_df_custom(spark, db_table, column_list, bus_date="1111-11-11"):
    if isinstance(column_list, list):
        # print("inside if loop")
        df = spark.read.table(db_table)
        return df.select(*column_list)
    elif isinstance(column_list, str) and column_list.upper() == "ALL":
        # print("inside else(all) loop")
        return spark.read.table(db_table)
    elif isinstance(column_list, dict):
        part_fields = column_list['part_fields']
        rnk_fields = column_list['rnk_fields']
        sel_fields = column_list['sel_fields']
        filter_con = column_list['filter_con']
        active_field = column_list['active_field']
        sql_query = ''' select * from (
            select *, rank() over (partition by {part} order by {rnk}) as rnk 
            from {tbl} 
            where {dt_field} <= '{dv}'
        ) T 
        where T.rnk = 1 and T.{filter} '''.format(
            part=part_fields,rnk=rnk_fields,tbl=db_table,dt_field=active_field,dv=bus_date,filter=filter_con
        )
        df = spark.sql(sql_query).select(list(map(lambda c: c.strip(), sel_fields.split(','))))
        return df
                
def stng_to_dt(x):
    if x is None:
        output = x
    else:
        strng = x[re.search("\d", x).start(0):8 + re.search("\d", x).start(0)]
        output = datetime.strptime(strng, '%Y%m%d')
    return output

stng_to_dt_udf = udf(lambda x: stng_to_dt(x), DateType())

def file_data(spark, props,file_name):
    df1=spark.read.table((props['SOURCE_DB']+ "." + '{file_name}').format(file_name=file_name)).withColumn('_timestamp',stng_to_dt_udf(col("file_name"))).\
        withColumn('oid_instid', split(col('UniqueKey'), '\.').getItem(1)).withColumn('oid_clsno', split(col('UniqueKey'), '\.').getItem(0))
    w_file_name = Window.partitionBy(col("oid_clsno"), col("oid_instid"), col("_timestamp")).orderBy(col("_timestamp").desc())
    #df_dl_conflowfile=df_dl_conflowfile.filter(("_timestamp<='{active_dt}'").format(active_dt=args.active_dt)).withColumn("rnk_conflowfile", rank().over(w_conflowfile)).filter("rnk_conflowfile=1").drop("rnk_conflowfile")
    fin_df=df1.withColumn("rnk_conflowfile", rank().over(w_file_name)).filter("rnk_conflowfile=1").drop("rnk_conflowfile")
    #fin_df.show()
    return fin_df

def convert_epoch_to_date(df, col_list,default_date='1899-12-30'):
    for key in col_list:
        expression = "date_add(base_date," + key + ")"
        df = df.withColumn("base_date", sf.lit(default_date).cast("date"))\
             .withColumn(key,sf.col(key).cast("int"))\
            .withColumn("temp_col",sf.expr(expression))\
            .drop(key).drop("base_date").withColumnRenamed("temp_col", key)
    return df

def cast_cols_to_date(df, cols, source_format, target_format):
    for i in cols:
        df = df.withColumn(i, sf.from_unixtime(sf.unix_timestamp(sf.col(i), source_format), target_format))
    return df

def convert_epoch_to_timestamp(df, col_list):
    for key in col_list:
        df = df.withColumn("base_timestamp",sf.lit("1899-12-30 00:00:00"))\
                .withColumn("temp_col", sf.from_unixtime(sf.unix_timestamp(sf.col('base_timestamp')) + (
                                                sf.col(key).cast("bigint") + 600) * 60))\
                .drop(key, "base_timestamp").withColumnRenamed("temp_col", key)

    return df
def add_record_status(delta_df,history_df,active_column):
    df_deleted = history_df.alias("n").join(delta_df.alias("h"), sf.col("n.rec_sha") == sf.col("h.rec_sha"),
                                                  "left_anti")
    df_deleted = df_deleted.withColumn(active_column, sf.lit('D'))
    df_inserted = delta_df.withColumn(active_column, sf.lit('I'))
    final_df = df_inserted.unionByName(df_deleted)
    return final_df

# It is a generic function to write the dataframe data into Hive table with partitions.
def insert_hub_table(hc, target_db, target_table, source_type, final_load):
    df = hc.read.table(target_db + "." + target_table)
    final_load_data = final_load.withColumn("source", lit(source_type.upper())).select(df.columns).distinct()
    table_name = target_db+"."+target_table
    print("final_load_data.write.insertInto("+table_name+")")
    final_load_data.write.insertInto(table_name)
    hc.catalog.refreshTable(target_db + "." + target_table)
