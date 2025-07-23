#!/bin/python

#####################################################
# File Name: hub_base.py
# Type: Pyspark
# Purpose: This module is the base class for all Hub layer processors.
#          It provides common functionality for loading, transforming, and saving data to the Hub layer.
#          Child classes only need to implement the transform method and specify the granularity keys.
#          The module also includes a method to exclude migrated claims from the data which can also be skipped.
# Created: 09/05/2025
# Last Updated: 09/05/2025
# Author: IDP Team
#####################################################

import json
from abc import ABC, abstractmethod
from argparse import Namespace
from typing import Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import broadcast, coalesce, col, desc, lit, rank
from pyspark.sql.window import Window

from src.run.custom_logging import Log4j
from src.utils.df_utils import (create_sha_field, date_time_now,
                                dt_format_convert, load_tables,
                                save_hive_overwrite, selective_df_custom)


class HubBase(ABC):
    """
    Base class for all Hub layer processors providing common functionality.
    Child classes only need to implement transform method, granularity keys, and hive_partitions if it's different from source, active_ym.
    """

    def __init__(
        self,
        hc: SparkSession,
        args: Namespace,
        granularity_keys: str = "",
        hive_partitions: str = "source,active_ym",
    ) -> None:
        """Initialize the base processor with common objects.

        Args:
            hc (SparkSession): Spark Session
            args (Namespace): Command line arguments
            granularity_keys (str): Granularity keys for the hub table, comma separated (e.g., source, clmnumber).
            hive_partitions (str, optional): Hive partitions for the hub table. Default to "source, active_ym".
        """
        self.hc = hc
        self.args = args
        self.log = Log4j(hc)
        self.granularity_keys = granularity_keys
        self.hive_partitions = hive_partitions
        self.exclude_migrated_claims = True

    def run(self) -> None:
        """
        Main entry point that orchestrates the ETL process.
        This method handles data loading, transformation, and saving.
        """
        print("arguments", self.args)
        self.validate()
        self.log.info(self.__class__.__name__, "In hub module run function")
        target_table = self.args.get("target_tbl")
        target_db = self.args.get("target_db")
        source = self.args.get("source_type").upper()
        db_env = self.args.get("env")[0]
        active_dt = self.args.get("active_dt")
        granularity_keys = self.granularity_keys.split(",")
        hive_partitions = self.hive_partitions

        with open(self.args.get("CONFIG_DIC_FILE_NAME"), "r") as json_file:
            table_columns = json.load(json_file)

        ds = load_tables(
            hc=self.hc, table_columns=table_columns, active_dt=active_dt, db_env=db_env,
            table_loader=selective_df_custom
        )

        # Call child class implementation of transform
        final_data = self.transform(source, ds)

        final_data = final_data.dropDuplicates()

        # read the table into the dataframe
        # Rec_Version is one - on first time hub table load
        # create a sha key on each record, except for active_dt, rec_version, load_dt, source
        dt_now = date_time_now()
        active_ym = dt_format_convert(active_dt, "%Y-%m-%d", "%Y-%m")
        final_load = (
            create_sha_field(final_data)
            .withColumn("active_dt", lit(active_dt))
            .withColumn("active_ym", lit(active_ym))
            .withColumn("load_dt", lit(dt_now))
            .withColumn("source_system_cd", lit(source))
        )

        if self.args.load_type.upper() == "HISTORY":
            # For history load, we don't compare the results, we just set the rec_version to 1 and insert all the records.
            # History load can be done in parallel for multiple active_dt values.
            # Use the `src/run/hub_version_loader.py` script to update the rec_version, and remove the duplicate records.
            final_load = final_load.withColumn("rec_version", lit(1))
        else:
            # Version Logic
            w = Window.partitionBy([col(key) for key in granularity_keys]).orderBy(
                desc("active_dt"), desc("rec_version")
            )

            # This reduces the amount of data processed in the window operation
            history_data = (
                self.hc.read.table(f"{target_db}.{target_table}")
                .filter(
                    (col("source") == source) & (col("active_dt") <= lit(active_dt))
                )
                .select(*granularity_keys, "rec_sha", "rec_version", "active_dt")
                .withColumn("rnk", rank().over(w))
                .filter(col("rnk") == 1)
                .select(
                    *[col(key).alias(f"src_{key}") for key in granularity_keys],
                    col("rec_sha").alias("src_rec_sha"),
                    col("rec_version"),
                )
                .cache()
            )

            scd_data = final_load.alias("n").join(
                history_data.alias("h"),
                col("h.src_rec_sha") == col("n.rec_sha"),
                "left_anti",
            )

            # Join to get the latest version number

            join_cols = [
                col(f"h.src_{key}") == col(f"n.{key}") for key in granularity_keys
            ]
            final_load = (
                scd_data.alias("n")
                .join(history_data.alias("h"), join_cols, "left_outer")
                .select(
                    [col("n." + c) for c in final_load.columns]
                    + [
                        (coalesce(col("h.rec_version"), lit(0)) + lit(1)).alias(
                            "rec_version"
                        )
                    ]
                )
            )

        # Save to Hive
        save_hive_overwrite(
            spark=self.hc,
            df=final_load,
            hive_db_name=target_db,
            hive_table_name=target_table,
            mode="append_partition",
            partition_column=hive_partitions,
            app_logger=self.log,
        )

    def validate(self) -> None:
        """
        Validate the input parameters. To be overridden by subclasses if needed.
        """
        if not self.granularity_keys:
            raise ValueError("Granularity keys are required.")
        if not self.args.get("target_tbl"):
            raise ValueError("Target table name is required.")
        if not self.args.get("target_db"):
            raise ValueError("Target database name is required.")
        if not self.args.get("source_type"):
            raise ValueError("Source type is required.")
        # if not self.args.get("active_dt"):
        #     raise ValueError("Active date is required.")
        if not self.args.get("env"):
            raise ValueError("Environment is required.")

    @abstractmethod
    def transform(self, source: str, ds: Dict[str, DataFrame]) -> DataFrame:
        """Transform source data to target schema. To be overridden by subclasses.

        Args:
            source: The source system type (PIC, TIM, TMF, etc.)
            ds (Dict[str, DataFrame]): Dictionary of loaded DataFrames with `df_` prefix.

        Returns:
            DataFrame: The transformed data
        """
        pass
