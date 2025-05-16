#!/bin/python
from pyspark.sql import SparkSession


class create_spark_session:
    def __init__(self, app_name, mode):
        self.app_name = app_name
        self.mode = "local[4]" if mode is None else mode
        print("application name: " + self.app_name)
        print("application run mode: " + self.mode)

    def get_sc_hc(self):
        spark = SparkSession \
            .builder \
            .appName(self.app_name) \
            .master(self.mode) \
            .enableHiveSupport() \
            .getOrCreate()
        sc = spark.sparkContext
        hc = spark
        hc.sql("set optimize.sort.dynamic.partitionining=true")
        hc.sql("set hive.exec.dynamic.partition.mode=nonstrict")
        hc.sql("set hive.exec.dynamic.partition=true")
        hc.sql("set hive.enforce.bucketing=false")
        hc.sql("set hive.vectorized.execution.enabled=true")
        hc.sql("set hive.enforce.sorting=false")
        hc.sql("set mapreduce.map.memory.mb=9000")
        hc.sql("set mapreduce.map.java.opts=-Xmx7200m")
        hc.sql("set mapreduce.reduce.memory.mb=10000")
        hc.sql("set mapreduce.reduce.java.opts=-Xmx10240m")
        hc.sql("set hive.exec.max.dynamic.partitions.pernode=1000")
        hc.sql(
            "create temporary function mask_first as 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFMaskShowFirstN'")
        hc.sql(
            "create temporary function mask_hash as 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFMaskHash'")
        hc.sql(
            "create temporary function custom_hash as 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFHash'")

        print("******************************************************************************************")
        print("Application ID : " + sc.applicationId)
        print("******************************************************************************************")
        return (sc, hc)

    def get_spark(self):
        spark = SparkSession \
            .builder \
            .appName(self.app_name) \
            .master(self.mode) \
            .enableHiveSupport() \
            .getOrCreate()
        spark.sql("set optimize.sort.dynamic.partitionining=true")
        spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
        spark.sql("set hive.exec.dynamic.partition=true")
        spark.sql("set hive.enforce.bucketing=false")
        spark.sql("set hive.vectorized.execution.enabled=true")
        spark.sql("set hive.enforce.sorting=false")
        spark.sql("set mapreduce.map.memory.mb=9000")
        spark.sql("set mapreduce.map.java.opts=-Xmx7200m")
        spark.sql("set mapreduce.reduce.memory.mb=10000")
        spark.sql("set mapreduce.reduce.java.opts=-Xmx10240m")
        spark.sql("set hive.exec.max.dynamic.partitions.pernode=1000")
        return spark
