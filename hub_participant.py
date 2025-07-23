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
from pyspark.sql.functions import (broadcast, col, concat_ws, current_date,
                                   date_format, hour, lpad, lit, minute,
                                   to_date, when, year, collect_list,
                                   upper, rpad, coalesce)
from pyspark.sql.types import IntegerType, StringType, DecimalType

from src.ltc.hub_base import HubBase
from src.run.custom_logging import Log4j


class HubPimsctpAccident(HubBase):
    """
    Implementation of CTP Accident Hub table using hybrid CARA+HubBase approach.
    This class extends HubBase but implements dictionary-driven transformations from CARA.
    """

    def __init__(self, hc: SparkSession, args: Namespace) -> None:
        """Initialize the CTP Accident Hub table processor.

        This class implements a hybrid approach combining CARA pattern (dictionary-driven
        configuration) and HubBase inheritance for standard SCD Type 2 handling and versioning.

        Args:
            hc (SparkSession): Spark Session
            args (Namespace): Command line arguments
        """
        # Define granularity keys from business requirements
        # These fields uniquely identify an entity for SCD Type 2 versioning
        granularity_keys = "acc_no"

        # Call parent constructor with granularity keys
        super().__init__(hc, args, granularity_keys=granularity_keys, hive_partitions="source,active_ym")


    def transform(self, source: str, ds: Dict[str, DataFrame]) -> DataFrame:
        """Transform source data to target schema.

        Args:
            source (str): The source system type
            ds (Dict[str, DataFrame]): Dictionary of loaded DataFrames

        Returns:
            DataFrame: The transformed data
        """
        self.log.info(self.__class__.__name__, "Starting accident data transformation")

        # addubg role to each dataframe before joining
        ds["dl_CTPParticipantCyclist"] = ds["dl_CTPParticipantCyclist"].withColumn("ROLE",lit("Bicyclist"))
        ds["dl_CTPParticipantOther"] = ds["dl_CTPParticipantOther"].withColumn("ROLE",lit("Other"))
        ds["dl_CTPParticipantPedestrian"] = ds["dl_CTPParticipantPedestrian"].withColumn("ROLE",lit("Pedestrian"))
        ds["dl_CTPParticipantVehicleOwner"] = ds["dl_CTPParticipantVehicleOwner"].withColumn("ROLE",lit("Owner"))
        ds["dl_CTPParticipantVehiclePax"] = ds["dl_CTPParticipantVehiclePax"].withColumn("ROLE",lit("Passenger"))
        ds["dl_CTPParticipantVehiclePaxDriver"] = ds["dl_CTPParticipantVehiclePaxDriver"].withColumn("ROLE",lit("Driver"))
        ds["dl_CTPParticipantWitness"] = ds["dl_CTPParticipantWitness"].withColumn("ROLE",lit("Witness"))
        # Start with base CTP Union of all participant table\
        base_df = ds["dl_CTPParticipantCyclist"].unionByName(ds["dl_CTPParticipantOther"], allowMissingColumns=True)
        base_df = base_df.unionByName(ds["dl_CTPParticipantPedestrian"], allowMissingColumns=True)
        base_df = base_df.unionByName(ds["dl_CTPParticipantVehicleOwner"], allowMissingColumns=True)
        base_df = base_df.unionByName(ds["dl_CTPParticipantVehiclePax"], allowMissingColumns=True)
        base_df = base_df.unionByName(ds["dl_CTPParticipantVehiclePaxDriver"], allowMissingColumns=True)
        base_df = base_df.unionByName(ds["dl_CTPParticipantWitness"], allowMissingColumns=True).alias('participant')

        # Join with accident
        base_df = base_df.join(
            ds["dl_CTPAccident"].alias("dl_CTPAccident"),
            on=[
                col("participant.myAccident_clsno") == col("dl_CTPAccident.oid_clsno"),
                col("participant.myAccident_instid") == col("dl_CTPAccident.oid_instid")
            ],
            how="left"
        )
        self.log.info(self.__class__.__name__, "Joined with accident table")

        # Join with police finding
        base_df = base_df.join(
            ds["dl_CTPPoliceFinding"].alias("dl_CTPPoliceFinding"),
            on=[
                col("participant.myPoliceFinding_clsno") == col("dl_CTPPoliceFinding.oid_clsno"),
                col("participant.myPoliceFinding_instid") == col("dl_CTPPoliceFinding.oid_instid")
            ],
            how="left"
        )
        self.log.info(self.__class__.__name__, "Joined with police finding table")

        # Join with state details
        base_df = base_df.join(
            ds["dl_TIMState"].alias("dl_TIMState"),
            on=[
                col("participant.myPoliceFinding_clsno") == col("dl_TIMState.oid_clsno"),
                col("participant.myPoliceFinding_instid") == col("dl_TIMState.oid_instid")
            ],
            how="left"
        )
        self.log.info(self.__class__.__name__, "Joined with state table")

        # Join with country details
        base_df = base_df.join(
            ds["dl_TIMCountry"].alias("dl_TIMCountry"),
            on=[
                col("participant.myCountry_instid") == col("dl_TIMCountry.oid_instid"),
                col("participant.myCountry_clsno") == col("dl_TIMCountry.oid_clsno")
            ],
            how="left"
        )
        self.log.info(self.__class__.__name__, "Joined with country")

        # Join with driver licence
        base_df = base_df.join(
            ds["dl_CTPDriversLicenceType"].alias("dl_CTPDriversLicenceType"),
            on=[
                col("participant.myDriversLicenceType_instid") == col("dl_CTPDriversLicenceType.oid_instid"),
                col("participant.myDriversLicenceType_clsno") == col("dl_CTPDriversLicenceType.oid_clsno")
            ],
            how="left"
        )
        self.log.info(self.__class__.__name__, "Joined with driver licence info")

        # creating data frame for drug type
        drugtest_df = ds['dl_CTPPoliceFinding'].alias('police_finding').join(
                          ds['dl_CTPDrugTest'].alias('drugtest'),
                        on=[
                            col("police_finding.oid_instid") == col("drugtest.myPoliceFinding_instid"),
                            col("police_finding.oid_clsno") == col("drugtest.myPoliceFinding_clsno")
                        ],
                        how="left"
                        )

        drugtest_df = drugtest_df.join(
            ds['dl_CTPDrugTestedType'].alias('drugtest_type'),
            on=[
                col("drugtest_type.oid_instid") == col("police_finding.oid_instid"),
                col("drugtest_type.oid_clsno") == col("police_finding.oid_clsno")
            ],
            how="left"
        )

        drugtest_df = drugtest_df.groupBy("police_finding.oid_instid","police_finding.oid_clsno") \
            .agg(concat_ws(", ", collect_list("dl_CTPDrugTestedType.aDescription")).alias("aDescription"),
                 concat_ws(", ", collect_list("dl_CTPDrugTest.aResult")).alias("aResult"))

        # Join with drug test info
        base_df = base_df.join(
            drugtest_df,
            on=[
                col("participant.myDriversLicenceType_instid") == col("drugtest_df.oid_instid"),
                col("participant.myPoliceFinding_clsno") == col("drugtest_df.oid_clsno")
            ],
            how="left"
        )

        self.log.info(self.__class__.__name__, "Joined with drug test info")

        # Join with Vehicle on Accident
        base_df = base_df.join(
            ds["dl_CTPVehicleOnAccident"].alias("veh_on_accident"),
            [
                col("participant.myVehicleOnAccident_instid") == col("veh_on_accident.oid_instid"),
                col("participant.myVehicleOnAccident_clsno") == col("veh_on_accident.oid_clsno")
            ],
            "left"
        )
        self.log.info(self.__class__.__name__, "Joined with Vehicle on Accident")

        # Join with seating position
        base_df = base_df.join(
            ds["dl_CTPSeatingPosition"].alias("seating_pos"),
            [
                col("participant.mySeatingPosition_instid") == col("seating_pos.oid_instid"),
                col("participant.mySeatingPosition_clsno") == col("seating_pos.oid_clsno")
            ],
            "left"
        )
        self.log.info(self.__class__.__name__, "Joined with seating position")

        # Join with policy claim
        base_df = base_df.join(
            ds["dl_TIMPolicyClaim"].alias("policy_claim"),
            [
                col("participant.myParticipant_instid") == col("policy_claim.oid_instid"),
                col("participant.myParticipant_clsno") == col("policy_claim.oid_clsno")
            ],
            "left"
        )
        self.log.info(self.__class__.__name__, "Joined with policy claim")

        # Join with contact type
        base_df = base_df.join(
            ds["dl_CTPContactTypeParticipant"].alias("contact_type"),
            [
                col("participant.myParticipant_instid") == col("contact_type.oid_instid"),
                col("participant.myParticipant_clsno") == col("contact_type.oid_clsno")
            ],
            "left"
        )
        self.log.info(self.__class__.__name__, "Joined with policy claim")

        # Calculate current year for accident year grouping
        from pyspark.sql.functions import year, current_date
        current_year = year(current_date()).cast(IntegerType()).eval()

        # Transform the data
        self.log.info(self.__class__.__name__, "Applying field transformations")
        final_data = base_df.select(
            col("dl_CTPAccident.aUniqueID").alias("acc_no"),
            col("dl_CTPVehicleOnAccident.aUniqueID").alias("veh_no"),
            # Primary identifier
            col("participant.aAccidentRoleOtherDetails").alias("pcpt_acc_role_oth_det_icf"),
            col("dl_CTPPoliceFinding.aQuantityConsumed").alias("pcpt_alcoh_qty_consum"),
            col("dl_CTPPoliceFinding.aIsAlcoholTest").alias("pcpt_alcoh_tested"),
            col("dl_CTPPoliceFinding.aBloodAnalysisTaken").alias("pcpt_blood_analysis"),
            col("dl_CTPPoliceFinding.aBloodAnalysisResult").alias("pcpt_blood_result"),
            col("dl_CTPPoliceFinding.aBreathAnalysisResult").alias("pcpt_breath_result"),
            col("dl_CTPPoliceFinding.aWCClaimNumber").alias("pcpt_wc_clm_no"),
            concat(col(rpad("dl_CTPAccident.aUniqueID",9,"0")),col(rpad(coalesce("dl_CTPVehicleOnAccident.aUniqueID",1),3,"0"))
            ,col(rpad("participant.aUniqueID",9,"0"))).alias("pcpt_acc_veh"),
            col("participant.aDrivingWhileSuspended").alias("pcpt_driv_while_susp"),
            when(col("dl_CTPPoliceFinding.aDrugAnalysisTaken") == 1, "Yes")
            .when(col("dl_CTPPoliceFinding.aDrugAnalysisTaken") == 2, "No")
            .otherwise("Unknown").alias("pcpt_drug_analysis"),
            col("drugtest_df.aDescription").alias("pcpt_drug_test"),
            col("drugtest_df.aResult").alias("pcpt_drug_test_res"),
            when(col("dl_CTPPoliceFinding.aAlcoholConsumed") == 1, "Yes")
            .when(col("dl_CTPPoliceFinding.aAlcoholConsumed") == 2, "No")
            .otherwise("Unknown").alias("pcpt_drugs_alcoh_involv_flg"),
            col("dl_CTPPoliceFinding.aFailToSubmit").alias("pcpt_fail_to_submit"),
            col("dl_TIMState.aName").alias("pcpt_identfcn_state_issud"),
            when(col(upper("dl_CTPDriversLicenceType.aDescription")) == 'PASSPORT', col("dl_TIMCountry.aName"))
            .otherwise(None).alias("pcpt_clmnt_paspt_cntry_of_issue"),
            col("dl_TIMCountry.aName").alias("pcpt_overseas_identfcn"),
            col("participant.ROLE").alias("pcpt_role"),
            col("participant.aDriversLicenseUnknown").alias("pcpt_unknown_identfcn"),
            col("participant.aUnlicensed").alias("pcpt_unlicenced"),
            col("participant.aWorkersCompEntitlement").alias("pcpt_wc_clm_lodged"),
            when(col("participant.ROLE") == "Driver", 1)
            .otherwise(0).alias("pcpt_driv_flg"),
            when(col("participant.ROLE") == "Driver", col("dl_CTPVehicleOnAccident.aUniqueID"))
            .otherwise(None).alias("pcpt_driv_veh_no"),
            col("participant.aDriversLicenseNumber").alias("pcpt_identfcn_no"),
            when((col("participant.ROLE").isin("Driver","Passenger") & col("participant.aIsInjured") == 1), col("participant.ROLE"))
            .otherwise(None).alias("pcpt_injur_party_acc_role"),
            col("participant.aIsInjured").alias("pcpt_injur_party_flg"),
            when(col(upper("dl_CTPDriversLicenceType.aDescription")) == 'DRIVERS LICENCE', col("dl_TIMState.aName"))
            .otherwise(None).alias("pcpt_license_state"),
            col("participant.aClaimNumber").alias("pcpt_mnging_insur_ctp_clm_no"),
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
            col("participant.aKey").alias("pcpt_con_id"),
            when((col("participant.ROLE") == "Driver" & col("dl_CTPSeatingPosition.aCode") == 6), 1)
            .otherwise(0).alias("pcpt_motrcyc_rider_flg"),
            when((col("participant.ROLE") == "Passenger" & col("dl_CTPSeatingPosition.aCode") == 16), 1)
            .otherwise(0).alias("pcpt_pillion_pass_flg")
        )

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
    processor = HubPimsctpAccident(hc, args)
    processor.run()