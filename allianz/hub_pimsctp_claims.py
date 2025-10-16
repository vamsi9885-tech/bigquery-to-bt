#!/bin/python

#####################################################
# File Name: hub_pimsctp_legal.py
# Type: Pyspark
# Purpose: This module implements a claim Legal table following hybrid approach.
# Created: 2025-08-22
# Author: Allianz Data Engineering
#####################################################

from argparse import Namespace
import sys
import yaml
from typing import Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, lit, when, coalesce, concat_ws, broadcast, to_date, date_format,
    row_number
)
from pyspark.sql.types import IntegerType, StringType
from pyspark.sql.window import Window
import pyspark.sql.functions as F

from src.claim.hub_base import HubBase
from src.run.custom_logging import Log4j


class HubPimsctpLegal(HubBase):
    """
    Implementation of Legal Hub table using hybrid CARA+HubBase approach.
    This class extends HubBase but implements dictionary-driven transformations from CARA.
    """

    def __init__(self, hc: SparkSession, args: Namespace, yaml_config_file="hub_pimsctp_ltc.yaml") -> None:
        """Initialize the Legal Hub table processor with configuration from YAML.

        Args:
            hc (SparkSession): Spark Session
            args (Namespace): Command line arguments
            yaml_config_file (str): Path to YAML configuration file
        """
        # Load properties from YAML configuration file
        properties = {}
        try:
            with open(yaml_config_file, 'r') as con:
                properties = yaml.safe_load(con)
                print(f"Successfully loaded configuration from {yaml_config_file}")
        except yaml.YAMLError as exc:
            print(f"Error parsing YAML configuration: {str(exc)}")
            sys.exit(1)
        except FileNotFoundError as exc:
            print(f"Configuration file not found: {yaml_config_file}")
            sys.exit(1)
        
        # Get configuration for this table (use uppercase table name as key)
        table_config = properties.get('HUB_PIMSCTP_LEGAL', {})
        
        # Granularity keys from specification document
        granularity_keys = "source_system_cd,clm_no,legl_aCode,legl_rank_first"
        
        # Update args with configuration properties (only environment configs)
        args.update(table_config)
        
        # Call parent constructor with granularity keys and partition configuration
        super().__init__(
            hc, 
            args, 
            granularity_keys=granularity_keys,
            hive_partitions="source_system_cd,active_ym"
        )
        
        # Now self.log is available from HubBase parent class
        self.log.info(self.__class__.__name__, f"Successfully initialized with configuration from {yaml_config_file}")

    def transform(self, source: str, ds: Dict[str, DataFrame]) -> DataFrame:
        """Transform source data to target schema with transformations in code.

        Args:
            source (str): The source system type
            ds (Dict[str, DataFrame]): Dictionary of loaded DataFrames

        Returns:
            DataFrame: The transformed data
        """
        self.log.info(self.__class__.__name__, "Implementing transformations in code")
        
        # Start with base table as driving dataframe
        base_df_key = "dl_TIMClaimContactType"
        
        # Verify base dataframe exists
        if base_df_key not in ds:
            self.log.error(self.__class__.__name__, f"Required base dataframe {base_df_key} not found in loaded datasets")
            sys.exit(1)
            
        # Get driving dataframe
        final_data = ds[base_df_key].alias("contact")
        self.log.info(self.__class__.__name__, f"Retrieved base dataframe {base_df_key}")
        
        # Apply window function to get latest records from driving table
        window_spec = Window.partitionBy(
            col("contact.oid_clsno"), 
            col("contact.oid_instid")
        ).orderBy(col("contact._timestamp").desc())
        final_data = final_data.withColumn("row_num", F.row_number().over(window_spec))
        final_data = final_data.filter(col("row_num") == 1).drop("row_num")
        final_data = final_data.filter(col("contact._operation") != "D")
        
        # Join with TIMClaimContactType
        claim_df = ds["dl_TIMPolicyClaim"].alias("claim")
        
        # Apply window function to contact type table for deduplication
        window_contact = Window.partitionBy(
            col("claim.oid_clsno"), 
            col("claim.oid_instid")
        ).orderBy(col("claim._timestamp").desc())
        claim_df = claim_df.withColumn("row_num", F.row_number().over(window_contact))
        claim_df = claim_df.filter(col("row_num") == 1).drop("row_num")
        claim_df = claim_df.filter(col("claim._operation") != "D")
        final_data = final_data.join(
            claim_df,
            (col("claim.oid_clsno") == col("contact.myClaim_clsno")) &
            (col("claim.oid_instid") == col("contact.myClaim_instid")),
            "inner"
        )
        self.log.info(self.__class__.__name__, "Applied inner join with TIMClaimContactType")
        
        # Join with TIMContactTypeSubcategory
        subcategory_df = ds["dl_TIMContactTypeSubcategory"].alias("subcat")
        
        # Apply window function to subcategory table
        window_subcat = Window.partitionBy(
            col("subcat.oid_clsno"), 
            col("subcat.oid_instid")
        ).orderBy(col("subcat._timestamp").desc())
        subcategory_df = subcategory_df.withColumn("row_num", F.row_number().over(window_subcat))
        subcategory_df = subcategory_df.filter(col("row_num") == 1).drop("row_num")
        subcategory_df = subcategory_df.filter(col("subcat._operation") != "D")
        
        final_data = final_data.join(
            subcategory_df,
            (col("contact.myContactTypeSubCategory_clsno") == col("subcat.oid_clsno")) &
            (col("contact.myContactTypeSubCategory_instid") == col("subcat.oid_instid")),
            "inner"
        )
        self.log.info(self.__class__.__name__, "Applied inner join with TIMContactTypeSubcategory")
        
        # Join with TIMContactTypeClaimLink
        claim_link_df = ds["dl_TIMContactTypeClaimLink"].alias("link")
        
        # Apply window function to claim link table
        window_link = Window.partitionBy(
            col("link.oid_clsno"), 
            col("link.oid_instid")
        ).orderBy(col("link._timestamp").desc())
        claim_link_df = claim_link_df.withColumn("row_num", F.row_number().over(window_link))
        claim_link_df = claim_link_df.filter(col("row_num") == 1).drop("row_num")
        claim_link_df = claim_link_df.filter(col("link._operation") != "D")
        
        final_data = final_data.join(
            claim_link_df,
            (col("contact.myContactType_clsno") == col("link.oid_clsno")) &
            (col("contact.myContactType_instid") == col("link.oid_instid")),
            "left"
        )
        self.log.info(self.__class__.__name__, "Applied left join with TIMContactTypeClaimLink")
        
        # Join with TIMLitigationReferralType using broadcast for small lookup table
        litigation_df = ds["dl_TIMLitigationReferralType"].alias("lit")
        
        # Apply window function to litigation referral table
        window_lit = Window.partitionBy(
            col("lit.oid_clsno"), 
            col("lit.oid_instid")
        ).orderBy(col("lit._timestamp").desc())
        litigation_df = litigation_df.withColumn("row_num", F.row_number().over(window_lit))
        litigation_df = litigation_df.filter(col("row_num") == 1).drop("row_num")
        litigation_df = litigation_df.filter(col("lit._operation") != "D")
        
        final_data = final_data.join(
            broadcast(litigation_df),
            (col("claim.myLitReferralType_clsno") == col("lit.oid_clsno")) &
            (col("claim.myLitReferralType_instid") == col("lit.oid_instid")),
            "left"
        )
        self.log.info(self.__class__.__name__, "Applied broadcast left join with TIMLitigationReferralType")
        
        # Apply field transformations with comprehensive null handling
        self.log.info(self.__class__.__name__, "Applying field transformations in code")
        
        # Create ranking windows for latest and first records
        window_first = Window.partitionBy(col("contact.myClaim_clsno"), 
                                          col("contact.myClaim_instid"), 
                                          col("contact.myContactTypeSubCategory_clsno"),
                                          col("contact.myContactTypeSubCategory_instid")
        ).orderBy(col("contact.aDateEngaged").asc_nulls_last(), col("contact._timestamp").asc_nulls_last(), col("contact.oid_instid").asc_nulls_last())
        
        final_data = final_data.withColumn("legl_rank_first", F.row_number().over(window_first))
        
        final_data = final_data.select(
            # Claim number
            coalesce(col("claim.aClaimNumber"), lit("")).alias("clm_no"),
            
            # Legal consultation date
            to_date(col("claim.aDateInitialConsultSolicitor"), "yyyy-MM-dd").alias("legl_consultatn_dt"),
            
            # Legal representation date
            to_date(col("contact.aDateEngaged"), "yyyy-MM-dd").alias("legl_rep_date"),
            
            # Legal subcategory code
            coalesce(col("subcat.aCode"), lit("")).alias("legl_aCode"),
            
            # Legal subcategory description
            coalesce(col("subcat.aDescription"), lit("")).alias("legl_subCategory"),
            
            # Legal contact type - concatenate ClaimLink prefix with subcategory description
            concat_ws("", 
                lit("ClaimLink - "),
                col("subcat.aDescription")
            ).alias("legl_contact_type"),
            
            # Contact ID
            col("link.aKey").cast(IntegerType()).alias("legl_Contact_Id"),
            
            # Defendant counsel
            col("claim.aLitDefendantCounsel").alias("legl_defendnt_counsel"),
            
            # Plaintiff counsel
            col("claim.aLitPlaintiffCounsel").alias("legl_plntif_counsel"),
            
            # Defendant referral type
            col("lit.aDescription").alias("legl_defendnt_refrl_type"),

            col("legl_rank_first").alias("legl_rank_first"),
            
            lit("PIMS").alias("source")
        )

        self.log.info(self.__class__.__name__, "Transformations completed successfully")
        return final_data
