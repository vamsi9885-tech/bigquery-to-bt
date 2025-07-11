import logging
from pyspark.sql import functions as sf
from pyspark.sql.functions import sha2, concat_ws, lit, date_format, to_date, from_unixtime, unix_timestamp, col, flatten, rank
from pyspark.sql.types import TimestampType
from pyspark.sql.window import Window

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def selective_df(spark, db_table, column_list, start_date=None, end_date=None, delta_date_format=None):
    if isinstance(column_list, list):
        df = spark.read.table(db_table)
        return df.select(*column_list)

    elif isinstance(column_list, str) and column_list.upper() == "ALL":
        return spark.read.table(db_table)

    elif isinstance(column_list, dict):
        rnk_fields = column_list.get('rnk_fields')
        sel_fields = column_list['sel_fields'].split(",")
        df = spark.read.table(db_table).select(*sel_fields)

        if column_list.get('filter_con', 0):
            df = df.filter(column_list['filter_con'])

        if start_date is not None:
            delta_column = column_list.get('active_field', '').strip()
            if delta_date_format is not None:
                filter_cond = f"(to_timestamp({delta_column}, '{delta_date_format}') >= to_timestamp('{start_date}', '{delta_date_format}') AND to_timestamp({delta_column}, '{delta_date_format}') < to_timestamp('{end_date}', '{delta_date_format}'))"
                df = df.filter(filter_cond)
            else:
                df = df.filter((df[delta_column] >= lit(start_date)) & (df[delta_column] < lit(end_date)))

        if start_date is None and end_date is not None:
            delta_column = column_list.get('active_field', '').strip()
            df = df.filter(df[delta_column] < lit(end_date))

        if rnk_fields is not None:
            part_fields = column_list.get('part_fields')
            part_concat_fields = column_list.get('part_concat_fields')
            rnk_fields = rnk_fields.split(",")
            order_fields = [df[x.split()[0]].asc() if "asc" in x else df[x.split()[0]].desc() for x in rnk_fields]

            if part_fields is not None:
                w = Window.partitionBy(*part_fields.split(",")).orderBy(*order_fields)
            else:
                w = Window.partitionBy(sf.concat_ws('', *part_concat_fields.split(","))).orderBy(*order_fields)

            if column_list.get('row_num') is not None:
                df = df.withColumn("rornk", sf.row_number().over(w)).filter("rornk=1").drop("rornk")
            else:
                df = df.withColumn("rnk", sf.rank().over(w)).filter("rnk=1").drop("rnk")

            df = df.dropDuplicates()
        return df

def create_sha_field(df, column_name, included_columns=[], num_bits=256):
    filtered_type = df.select(*included_columns).dtypes
    return df.withColumn(column_name, sha2(concat_ws("|", *[df[x[0]] for x in filtered_type]), num_bits))

def add_audit_columns(df, schema, excluded_columns, source_dt_format="yyyy-MM-dd"):
    sel_list = [x for x in df.columns if x not in excluded_columns]
    df = create_sha_field(df, "rec_sha", sel_list)
    if schema:
        df = df.withColumn("source_system_cd", lit(schema))
    df = df.withColumn("active_ym", date_format(to_date(df.active_dt, source_dt_format), "yyyy-MM"))
    df = df.withColumn("load_dt", lit(current_timestamp()))
    return df

def get_active_date(df, active_dt_column, input_date_format="yyyyMMDD", output_date_format="yyyy-MM-dd"):
    df = df.withColumn("active_dt", from_unixtime(unix_timestamp(df[active_dt_column], input_date_format), output_date_format).cast(TimestampType()))
    df = df.withColumn("active_ym", date_format(to_date(col("active_dt"), "yyyy-MM-dd"), "yyyy-MM"))
    return df

def get_rec_version(df, key_cols):
    window_spec = Window.partitionBy(*[col(key) for key in key_cols]).orderBy(col("trtmt_care_advised_dt"))
    return df.withColumn("record_version_no", rank().over(window_spec))

def order_cols(my_cols, tbl_cols):
    return [col(c) if c in my_cols else lit(None).alias(c) for c in tbl_cols]
