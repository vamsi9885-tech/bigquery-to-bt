#!/bin/python

#####################################################
# File Name: hub_participant.py
# Type: Pyspark
# Purpose: This module is for lake to hub data transfer
# using hub_base class for participant requirement
# Created: 09/05/2025
# Last Updated: 09/05/2025
# Author: IDP Team
#####################################################


from argparse import Namespace
from typing import Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (col, concat_ws, lit, when, collect_list,
                                   concat, upper, rpad, coalesce)

from src.claim.hub_base import HubBase
import yaml, sys


class HubPimsctpParticipant(HubBase):
    """
    Implementation of CTP Accident Hub table using hybrid CARA+HubBase approach.
    This class extends HubBase but implements dictionary-driven transformations from CARA.
    """

    def __init__(self, hc: SparkSession, args: Namespace, yaml_config_file = "hub_pimsctp_ltc.yaml") -> None:
        """Initialize the CTP Accident Hub table processor.

        This class implements a hybrid approach combining CARA pattern (dictionary-driven
        configuration) and HubBase inheritance for standard SCD Type 2 handling and versioning.

        Args:
            hc (SparkSession): Spark Session
            args (Namespace): Command line arguments
        """
        # Define granularity keys from business requirements


        # read required
        properties = {}
        with open(yaml_config_file, 'r') as con:
            try:
                properties = yaml.safe_load(con)
            except yaml.YAMLError as exc:
                sys.exit(1)

        # These fields uniquely identify an entity for SCD Type 2 versioning
        granularity_keys = properties['HUB_PIMSCTP_PARTICIPANT'].get("granularity_keys", "source_system_cd,acc_no,veh_no,pcpt_acc_veh,pcpt_role")

        args.update(properties['HUB_PIMSCTP_PARTICIPANT'])
        # Call parent constructor with granularity keys
        super().__init__(hc, args, granularity_keys=granularity_keys, hive_partitions="source_system_cd,active_ym")

    def add_null_columns(self, df: DataFrame) -> DataFrame:
        """Add null columns to the DataFrame for missing fields."""

        df = df.withColumn("myVehicleOnAccident_clsno",lit(None))
        df = df.withColumn("myVehicleOnAccident_instid",lit(None))
        df = df.withColumn("mySeatingPosition_clsno",lit(None))
        df = df.withColumn("mySeatingPosition_instid",lit(None))
        return df

    def join_driving_table_with_other_tables(self, ds: Dict[str, DataFrame], base_df: DataFrame) -> DataFrame:
        """Join the base DataFrame with other related tables to enrich the data."""

        def return_drugtest_data():
            """Create a DataFrame for drug test data."""
            drug_test_data = ds['dl_CTPPoliceFinding'].alias('dl_CTPPoliceFinding').join(
                ds['dl_CTPDrugTest'].alias('dl_CTPDrugTest'),
                on=[
                    col("dl_CTPPoliceFinding.oid_instid") == col("dl_CTPDrugTest.myPoliceFinding_instid"),
                    col("dl_CTPPoliceFinding.oid_clsno") == col("dl_CTPDrugTest.myPoliceFinding_clsno")
                ],
                how="left"
            )

            drug_test_data = drug_test_data.join(
                ds['dl_CTPDrugTestedType'].alias('dl_CTPDrugTestedType'),
                on=[
                    col("dl_CTPDrugTestedType.oid_instid") == col("dl_CTPDrugTest.myDrugTestType_instid"),
                    col("dl_CTPDrugTestedType.oid_clsno") == col("dl_CTPDrugTest.myDrugTestType_clsno")
                ],
                how="left"
            )

            drug_test_data = drug_test_data.groupBy("dl_CTPDrugTest.myPoliceFinding_instid","dl_CTPDrugTest.myPoliceFinding_clsno") \
                .agg(concat_ws(", ", collect_list("dl_CTPDrugTestedType.aDescription")).alias("drugtest_aDescription"),
                     concat_ws(", ", collect_list("dl_CTPDrugTest.aResult")).alias("drugtest_aResult"))
            return drug_test_data


        # Join with accident
        join_cond = [
            col("participant.myAccident_clsno") == col("dl_CTPAccident.oid_clsno"),
            col("participant.myAccident_instid") == col("dl_CTPAccident.oid_instid")
        ]
        base_df = self.join_table(ds, base_df,join_table="dl_CTPAccident", join_condition=join_cond, join_type="left" )

        self.log.info(self.__class__.__name__, "Joined with accident table")    
        # Join with police finding
        join_cond = [
            col("participant.myPoliceFinding_clsno") == col("dl_CTPPoliceFinding.oid_clsno"),
            col("participant.myPoliceFinding_instid") == col("dl_CTPPoliceFinding.oid_instid")
        ]
        base_df = self.join_table(df_dict=ds, driving_table=base_df, join_table="dl_CTPPoliceFinding", join_condition=join_cond, join_type="left")

        self.log.info(self.__class__.__name__, "Joined with police finding table")

        # Join with state details
        join_cond = [
            col("participant.myDriversLicenseState_clsno") == col("dl_TIMState.oid_clsno"),
            col("participant.myDriversLicenseState_instid") == col("dl_TIMState.oid_instid")
        ]
        base_df = self.join_table(df_dict=ds, driving_table=base_df, join_table="dl_TIMState", join_condition=join_cond, join_type="left")

        self.log.info(self.__class__.__name__, "Joined with state table")

        # Join with country details
        join_cond = [
            col("dl_TIMState.myCountry_instid") == col("dl_TIMCountry.oid_instid"),
            col("dl_TIMState.myCountry_clsno") == col("dl_TIMCountry.oid_clsno")
        ]
        base_df = self.join_table(df_dict=ds, driving_table=base_df, join_table="dl_TIMCountry", join_condition=join_cond, join_type="left")
        self.log.info(self.__class__.__name__, "Joined with country")

        # Join with driver licence
        join_cond = [
            col("participant.myDriversLicenceType_instid") == col("dl_CTPDriversLicenceType.oid_instid"),
            col("participant.myDriversLicenceType_clsno") == col("dl_CTPDriversLicenceType.oid_clsno")
        ]
        base_df = self.join_table(df_dict=ds, driving_table=base_df, join_table="dl_CTPDriversLicenceType", join_condition=join_cond, join_type="left")

        self.log.info(self.__class__.__name__, "Joined with driver licence info")

        drug_test_df = return_drugtest_data()

        # Join with drug test info
        base_df = base_df.join(
            drug_test_df,
            on=[
                col("participant.myPoliceFinding_instid") == col("dl_CTPDrugTest.myPoliceFinding_instid"),
                col("participant.myPoliceFinding_clsno") == col("dl_CTPDrugTest.myPoliceFinding_clsno")
            ],
            how="left"
        )

        self.log.info(self.__class__.__name__, "Joined with drug test info")

        # Join with Vehicle on Accident
        join_cond = [
            col("participant.myVehicleOnAccident_instid") == col("dl_CTPVehicleOnAccident.oid_instid"),
            col("participant.myVehicleOnAccident_clsno") == col("dl_CTPVehicleOnAccident.oid_clsno")
        ]
        base_df = self.join_table(df_dict=ds, driving_table=base_df, join_table="dl_CTPVehicleOnAccident", join_condition=join_cond, join_type="left")
        self.log.info(self.__class__.__name__, "Joined with Vehicle on Accident")

        # Join with seating position
        join_cond = [
            col("participant.mySeatingPosition_instid") == col("dl_CTPSeatingPosition.oid_instid"),
            col("participant.mySeatingPosition_clsno") == col("dl_CTPSeatingPosition.oid_clsno")
        ]
        base_df = self.join_table(df_dict=ds, driving_table=base_df, join_table="dl_CTPSeatingPosition", join_condition=join_cond, join_type="left")
        self.log.info(self.__class__.__name__, "Joined with seating position")

        # Join with policy claim
        join_cond = [
            col("participant.myParticipant_instid") == col("dl_TIMPolicyClaim.oid_instid"),
            col("participant.myParticipant_clsno") == col("dl_TIMPolicyClaim.oid_clsno")
        ]
        base_df = self.join_table(df_dict=ds, driving_table=base_df, join_table="dl_TIMPolicyClaim", join_condition=join_cond, join_type="left")
        self.log.info(self.__class__.__name__, "Joined with policy claim")

        # Join with contact type
        join_cond = [
            col("participant.myParticipant_instid") == col("dl_CTPContactTypeParticipant.oid_instid"),
            col("participant.myParticipant_clsno") == col("dl_CTPContactTypeParticipant.oid_clsno")
        ]
        base_df = self.join_table(df_dict=ds, driving_table=base_df, join_table="dl_CTPContactTypeParticipant", join_condition=join_cond, join_type="left")
        self.log.info(self.__class__.__name__, "Joined with policy claim")

        return base_df

    def read_cte_df_return_base_df(self, ds: Dict[str, DataFrame]) -> DataFrame:
        # adding role to each dataframe before joining
        self.log.info(self.__class__.__name__, "Starting participant data transformation")

        ds["dl_CTPParticipantCyclist"] = self.add_null_columns(ds["dl_CTPParticipantCyclist"])
        ds["dl_CTPParticipantOther"] = self.add_null_columns(ds["dl_CTPParticipantOther"])
        ds["dl_CTPParticipantPedestrian"] = self.add_null_columns(ds["dl_CTPParticipantPedestrian"])
        ds["dl_CTPParticipantWitness"] = self.add_null_columns(ds["dl_CTPParticipantWitness"])
        ds["dl_CTPParticipantCyclist"] = ds["dl_CTPParticipantCyclist"].withColumn("ROLE",lit("Bicyclist"))
        ds["dl_CTPParticipantOther"] = ds["dl_CTPParticipantOther"].withColumn("ROLE",lit("Other"))
        ds["dl_CTPParticipantPedestrian"] = ds["dl_CTPParticipantPedestrian"].withColumn("ROLE",lit("Pedestrian"))
        ds["dl_CTPParticipantVehicleOwner"] = ds["dl_CTPParticipantVehicleOwner"].withColumn("ROLE",lit("Owner"))
        ds["dl_CTPParticipantVehiclePax"] = ds["dl_CTPParticipantVehiclePax"].withColumn("ROLE",lit("Passenger"))
        ds["dl_CTPParticipantVehiclePaxDriver"] = ds["dl_CTPParticipantVehiclePaxDriver"].withColumn("ROLE",lit("Driver"))
        ds["dl_CTPParticipantWitness"] = ds["dl_CTPParticipantWitness"].withColumn("ROLE",lit("Witness"))
        # Start with base CTP Union of all participant table\
        base_df = ds["dl_CTPParticipantCyclist"].unionByName(ds["dl_CTPParticipantOther"])
        base_df = base_df.unionByName(ds["dl_CTPParticipantPedestrian"])
        base_df = base_df.unionByName(ds["dl_CTPParticipantVehicleOwner"])
        base_df = base_df.unionByName(ds["dl_CTPParticipantVehiclePax"])
        base_df = base_df.unionByName(ds["dl_CTPParticipantVehiclePaxDriver"])
        base_df = base_df.unionByName(ds["dl_CTPParticipantWitness"]).alias('participant')

        return base_df

    def return_selected_columns_from_final_dataframe(self, base_df: DataFrame) -> DataFrame:
        """Select and rename columns for the final output.

        Args:
            base_df (DataFrame): The base DataFrame after joining tables

        Returns:
            DataFrame: The DataFrame with selected and renamed columns
        """
        # Transform the data
        self.log.info(self.__class__.__name__, "Applying field transformations")
        final_data = base_df.select(
            col("dl_CTPAccident.aUniqueID").alias("acc_no"),
            col("dl_CTPVehicleOnAccident.aUniqueID").alias("veh_no"),
            # Primary identifier
            col("participant.aAccidentRoleOtherDetails").alias("pcpt_acc_role_oth_det"),
            col("participant.aAccidentRoleOtherDetails").alias("pcpt_acc_role_det_icf"),
            col("dl_CTPPoliceFinding.aQuantityConsumed").alias("pcpt_alcoh_qty_consum"),
            col("dl_CTPPoliceFinding.aIsAlcoholTest").alias("pcpt_alcoh_tested"),
            col("dl_CTPPoliceFinding.aBloodAnalysisTaken").alias("pcpt_blood_analysis"),
            col("dl_CTPPoliceFinding.aBloodAnalysisResult").alias("pcpt_blood_result"),
            col("dl_CTPPoliceFinding.aBreathAnalysisTaken").alias("pcpt_breath_analysis"),
            col("dl_CTPPoliceFinding.aBreathAnalysisResult").alias("pcpt_breath_result"),
            col("dl_CTPPoliceFinding.aWCClaimNumber").alias("pcpt_wc_clm_no"),
            concat(rpad(col("dl_CTPAccident.aUniqueID"),9,"0"),rpad(coalesce(col("dl_CTPVehicleOnAccident.aUniqueID"),lit("1")),3,"0")
                   ,rpad(col("participant.aUniqueID"),9,"0")).alias("pcpt_acc_veh"),
            col("participant.aDrivingWhileSuspended").alias("pcpt_driv_while_susp"),
            when(col("dl_CTPPoliceFinding.aDrugAnalysisTaken") == 1, "Yes")
            .when(col("dl_CTPPoliceFinding.aDrugAnalysisTaken") == 2, "No")
            .otherwise("Unknown").alias("pcpt_drug_analysis"),
            col("drugtest_aDescription").alias("pcpt_drug_test"),
            col("drugtest_aResult").alias("pcpt_drug_test_res"),
            when(col("dl_CTPPoliceFinding.aAlcoholConsumed") == 1, "Yes")
            .when(col("dl_CTPPoliceFinding.aAlcoholConsumed") == 2, "No")
            .otherwise("Unknown").alias("pcpt_drugs_alcoh_involv_flg"),
            col("dl_CTPPoliceFinding.aFailToSubmit").alias("pcpt_fail_to_submit"),
            col("participant.aDriversLicenseExpiry").alias("pcpt_identfcn_expiry_dt"),
            col("dl_TIMState.aName").alias("pcpt_identfcn_state_issud"),
            col("dl_CTPPoliceFinding.aIncomeProtectionInsuranceAtTimeOfAcc").alias("pcpt_income_protcn_insur"),
            when(upper(col("dl_CTPDriversLicenceType.aDescription")) == 'PASSPORT', col("dl_TIMCountry.aName"))
            .otherwise(None).alias("pcpt_clmnt_paspt_cntry_of_issue"),
            col("dl_TIMCountry.aName").alias("pcpt_overseas_identfcn"),
            col("participant.ROLE").alias("pcpt_role"),
            col("participant.aDriversLicenseUnknown").alias("pcpt_unknown_identfcn"),
            col("participant.aUnlicensed").alias("pcpt_unlicenced"),
            col("dl_CTPPoliceFinding.aWorkersCompEntitlement").alias("pcpt_wc_clm_lodged"),
            when(col("participant.ROLE") == "Driver", 1)
            .otherwise(0).alias("pcpt_driv_flg"),
            when(col("participant.ROLE") == "Driver", col("dl_CTPVehicleOnAccident.aUniqueID"))
            .otherwise(None).alias("pcpt_driv_veh_no"),
            col("participant.aDriversLicenseNumber").alias("pcpt_identfcn_no"),
            when((col("participant.ROLE").isin("Driver","Passenger")) & (col("participant.aIsInjured") == 1), col("participant.ROLE"))
            .otherwise(None).alias("pcpt_injur_party_acc_role"),
            col("participant.aIsInjured").alias("pcpt_injur_party_flg"),
            when(upper(col("dl_CTPDriversLicenceType.aDescription")) == 'DRIVERS LICENCE', col("dl_TIMState.aName"))
            .otherwise(None).alias("pcpt_license_state"),
            col("dl_TIMPolicyClaim.aClaimNumber").alias("pcpt_mnging_insur_ctp_clm_no"),
            when(col("participant.ROLE") == "Owner", 1)
            .otherwise(0).alias("pcpt_owner_flg"),
            when(col("participant.ROLE") == "Owner", col("dl_CTPVehicleOnAccident.aUniqueID"))
            .otherwise(None).alias("pcpt_owner_veh_no"),
            when(col("participant.ROLE") == "Passenger", 1)
            .otherwise(0).alias("pcpt_pass_flg"),
            when(col("participant.ROLE") == "Passenger", col("dl_CTPSeatingPosition.aDescription"))
            .otherwise(None).alias("pcpt_pass_loc"),
            when(col("participant.ROLE") == "Passenger", col("dl_CTPVehicleOnAccident.aUniqueID"))
            .otherwise(None).alias("pcpt_pass_veh_no"),
            col("dl_CTPDriversLicenceType.aDescription").alias("pcpt_type_identfcn"),
            when(col("participant.ROLE") == "Witness", 1)
            .otherwise(0).alias("pcpt_witns_flg"),
            col("dl_CTPContactTypeParticipant.aKey").alias("pcpt_con_id"),
            when((col("participant.ROLE") == "Driver") & (col("dl_CTPSeatingPosition.aCode") == 6), 1)
            .otherwise(0).alias("pcpt_motrcyc_rider_flg"),
            when((col("participant.ROLE") == "Passenger") & (col("dl_CTPSeatingPosition.aCode") == 16), 1)
            .otherwise(0).alias("pcpt_pillion_pass_flg")
        )
        return final_data

    def transform(self, source: str, ds: Dict[str, DataFrame]) -> DataFrame:
        """Transform source data to target schema.

        Args:
            source (str): The source system type
            ds (Dict[str, DataFrame]): Dictionary of loaded DataFrames

        Returns:
            DataFrame: The transformed data
        """

        base_df = self.read_cte_df_return_base_df(ds)
        base_df = self.join_driving_table_with_other_tables(ds, base_df)
        final_data = self.return_selected_columns_from_final_dataframe(base_df)

        self.log.info(self.__class__.__name__, "Transformation completed successfully")

        return final_data


# Entry point function
def hub_participant_processor(hc: SparkSession, args: Namespace, app_logger):
    """
    Entry point function for the hub_injuredworker_bsmart module.

    Args:
        sc: Spark Context
        hc: SparkSession
        args: Command line arguments
    """
    processor = HubPimsctpParticipant(hc, args)
    processor.run()