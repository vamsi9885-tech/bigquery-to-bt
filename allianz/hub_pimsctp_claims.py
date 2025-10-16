#!/bin/python

#####################################################
# File Name: hub_pimsctp_claims.py
# Type: Pyspark
# Purpose: This module implements a claim table following hybrid approach.
# Created: 2025-08-25
# Author: GitHub Copilot
#####################################################

from argparse import Namespace
import sys
import yaml
from typing import Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, lit, when, coalesce, to_date, trim, concat_ws, broadcast, lpad, date_add, last_day,
    month, year, current_date, lower
)
from pyspark.sql.types import IntegerType, StringType, DecimalType
from src.claim.hub_base import HubBase


class HubPimsctpClaims(HubBase):
    """
    Implementation of a table using hybrid CARA+HubBase approach.
    This class extends HubBase but implements dictionary-driven transformations from CARA.
    """

    def __init__(self, hc: SparkSession, args: Namespace,
                 yaml_config_file="hub_pimsctp_ltc.yaml") -> None:
        """Initialize the table processor with configuration from YAML.

        Args:
            hc (SparkSession): Spark Session
            args (Namespace): Command line arguments
            yaml_config_file (str): Path to YAML configuration file
        """
        properties = {}
        try:
            with open(yaml_config_file, 'r') as con:
                properties = yaml.safe_load(con)
                print(f"Successfully loaded configuration from {yaml_config_file}")
        except yaml.YAMLError as exc:
            print(f"Error parsing YAML configuration: {str(exc)}")
            sys.exit(1)
        except FileNotFoundError:
            print(f"Configuration file not found: {yaml_config_file}")
            sys.exit(1)

        table_config = properties.get('HUB_PIMSCTP_CLAIMS', {})

        granularity_keys = table_config.get("granularity_keys", "clm_no")

        args.update(table_config)

        super().__init__(
            hc,
            args,
            granularity_keys=granularity_keys,
            hive_partitions="source_system_cd,active_ym"
        )

        self.log.info(self.__class__.__name__, f"Successfully initialized with configuration from {yaml_config_file}")

    def return_final_df_after_joining_tables(self, ds: Dict[str, DataFrame], base_df_key) -> DataFrame:
        """Join all necessary source tables to create the base DataFrame for transformations.

        Args:
            ds (Dict[str, DataFrame]): Dictionary of loaded DataFrames

        Returns:
            DataFrame: The joined base DataFrame
            :param ds:
            :param base_df_key:
        """
        final_data = ds[base_df_key].alias("dl_TIMPolicyClaim")

        # Join with TIMPolicy to get policy information
        join_cond = [
            (col("dl_TIMPolicyClaim.myPolicy_clsno") == col("dl_TIMPolicy.oid_clsno")) & (
                    col("dl_TIMPolicyClaim.myPolicy_instid") == col("dl_TIMPolicy.oid_instid"))
        ]
        final_data = self.join_table(df_dict=ds, driving_table=final_data, join_table="dl_TIMPolicy",
                                     join_condition=join_cond, join_type="left")
        self.log.info(self.__class__.__name__, "Applied left join with dl_TIMPolicy")

        # Join with decline and party information
        join_cond = [
            (col("dl_TIMPolicyClaim.myReasonForDecline_clsno") == col("dl_TIMReasonForDecline.oid_clsno")) & (
                    col("dl_TIMPolicyClaim.myReasonForDecline_instid") == col("dl_TIMReasonForDecline.oid_instid"))
        ]
        final_data = self.join_table(df_dict=ds, driving_table=final_data, join_table="dl_TIMReasonForDecline",
                                     join_condition=join_cond, join_type="left")
        self.log.info(self.__class__.__name__, "Applied left joins with decline tables")

        # Join with claim status and profile information - using broadcast for lookup tables

        final_data = final_data.join(broadcast(ds["dl_TIMClaimStatus"].alias("dl_TIMClaimStatus")),
                                     (col("dl_TIMPolicyClaim.myClaimStatus_clsno") == col(
                                         "dl_TIMClaimStatus.oid_clsno")) & (
                                             col("dl_TIMPolicyClaim.myClaimStatus_instid") == col(
                                         "dl_TIMClaimStatus.oid_instid")), "left")

        final_data = final_data.join(broadcast(ds["dl_CARClaimProfileCode"].alias("dl_CARClaimProfileCode")),
                                     (col("dl_TIMPolicyClaim.myClaimProfile_clsno") == col(
                                         "dl_CARClaimProfileCode.oid_clsno")) & (
                                             col("dl_TIMPolicyClaim.myClaimProfile_instid") == col(
                                         "dl_CARClaimProfileCode.oid_instid")), "left")

        self.log.info(self.__class__.__name__, "Applied broadcast left joins with claim status and profile tables")

        # Join with address and location information - using broadcast for small lookup tables
        self.log.info(self.__class__.__name__,
                      "Applied broadcast left joins with address, party type and lookup tables")

        # Join with status, status reopen and history information
        join_cond = [
            (col("dl_TIMPolicyClaim.oid_clsno") == col("dl_TIMProfileHistoryStatus.myClaim_clsno")) & (
                    col("dl_TIMPolicyClaim.oid_instid") == col("dl_TIMProfileHistoryStatus.myClaim_instid"))
        ]
        final_data = self.join_table(df_dict=ds, driving_table=final_data, join_table="dl_TIMProfileHistoryStatus",
                                     join_condition=join_cond, join_type="left")

        join_cond = [
            (col("dl_TIMClaimStatusHistoryReopen.oid_clsno") == col(
                "dl_TIMProfileHistoryStatus.myStatusChangeReason_clsno")) & (
                    col("dl_TIMClaimStatusHistoryReopen.oid_instid") == col(
                "dl_TIMProfileHistoryStatus.myStatusChangeReason_instid"))
        ]
        final_data = self.join_table(df_dict=ds, driving_table=final_data,
                                     join_table="dl_TIMClaimStatusHistoryReopen",
                                     join_condition=join_cond, join_type="left")
        join_cond = [
            (col("dl_TIMClaimStatusHistoryFinalise.oid_clsno") == col(
                "dl_TIMProfileHistoryStatus.myStatusChangeReason_clsno")) & (
                    col("dl_TIMClaimStatusHistoryFinalise.oid_instid") == col(
                "dl_TIMProfileHistoryStatus.myStatusChangeReason_instid"))
        ]
        final_data = self.join_table(df_dict=ds, driving_table=final_data,
                                     join_table="dl_TIMClaimStatusHistoryFinalise",
                                     join_condition=join_cond, join_type="left")

        self.log.info(self.__class__.__name__, "Applied left joins with status history tables")



        # Join with accident, contact and assessment information
        join_cond = [
            (col("dl_CTPAccident.oid_clsno") == col("dl_TIMPolicyClaim.myAccident_clsno")) & (
                    col("dl_CTPAccident.oid_instid") == col("dl_TIMPolicyClaim.myAccident_instid"))
        ]
        final_data = self.join_table(df_dict=ds, driving_table=final_data, join_table="dl_CTPAccident",
                                     join_condition=join_cond, join_type="left")

        join_cond = [
            (col("dl_TIMAddress.oid_clsno") == col("dl_CTPAccident.myAccidentLocation_clsno")) & (
                    col("dl_TIMAddress.oid_instid") == col("dl_CTPAccident.myAccidentLocation_instid"))
        ]
        final_data = self.join_table(df_dict=ds, driving_table=final_data, join_table="dl_TIMAddress",
                                     join_condition=join_cond, join_type="left")

        join_cond = [
            (col("dl_TIMState.oid_clsno") == col("dl_TIMAddress.myState_clsno")) & (
                    col("dl_TIMState.oid_instid") == col("dl_TIMAddress.myState_instid"))
        ]
        final_data = self.join_table(df_dict=ds, driving_table=final_data, join_table="dl_TIMState",
                                     join_condition=join_cond, join_type="left")


        self.log.info(self.__class__.__name__, "Applied left joins with accident, contact and assessment tables")

        # Join with remaining tables

        join_cond = [
            (col("dl_TIMLiabilityRejectReason.oid_clsno") == col("dl_TIMPolicyClaim.myLiabilityRejectReason_clsno")) & (
                    col("dl_TIMLiabilityRejectReason.oid_instid") == col(
                "dl_TIMPolicyClaim.myLiabilityRejectReason_instid"))
        ]
        final_data = self.join_table(df_dict=ds, driving_table=final_data, join_table="dl_TIMLiabilityRejectReason",
                                     join_condition=join_cond, join_type="left")

        self.log.info(self.__class__.__name__, "Applied left joins with remaining tables")

        return final_data

    def return_selected_columns_from_final_dataframe(self, df: DataFrame, source: str = "PIMS") -> DataFrame:
        """Apply field-level transformations to the joined DataFrame.

        Args:
            :param df:
            :param source:

        Returns:
            DataFrame: The DataFrame with transformed fields
        """
        transformed_df = df.select(
            # Primary business keys
            coalesce(col("dl_TIMPolicyClaim.aClaimNumber"), lit("")).alias("clm_no"),
            coalesce(lit(None).cast(StringType())).alias("clm_key"),
            coalesce(col("dl_CTPAccident.aUniqueID").cast(StringType())).alias("acc_no"),
            lit(None).cast(StringType()).alias("clm_notifcn_flg"),
            lit(None).cast(StringType()).alias("clm_racq_yes_no_flg"),
            coalesce(col("dl_TIMReasonForDecline.aDescription"), lit("")).alias("clm_rs_for_decline"),
            coalesce(col("dl_TIMPolicyClaim.aDoesS5Apply"), lit("")).alias("clm_s5_complnce_does_s5_apply"),
            to_date(col("dl_TIMPolicyClaim.aDateIniialConsultGP"), "yyyy-MM-dd").alias("clm_initial_consult_gp_dt"),
            to_date(col("dl_TIMPolicyClaim.aDateInitialConsultSolicitor"), "yyyy-MM-dd").alias(
                "clm_initial_consult_solicitor_dt"),
            to_date(col("dl_TIMPolicyClaim.aDateLimitationExtendedTo"), "yyyy-MM-dd").alias(
                "clm_limitatn_extnded_to_dt"),
            to_date(col("dl_TIMPolicyClaim.aAIFRequestedDate"), "yyyy-MM-dd").alias("clm_aif_reqst_dt"),
            coalesce(col("dl_TIMPolicyClaim.aHasClaimFormEntryAssistance"), lit("")).alias(
                "clm_assistnce_completng_icf"),
            to_date(col("dl_TIMPolicyClaim.aDateANFReported"), "yyyy-MM-dd").alias("clm_acc_notifcn_rcvd_dt"),
            coalesce(col("dl_TIMPolicyClaim.aIsEscalatedClaim"), lit("")).alias("clm_escalated"),
            coalesce(col("dl_TIMClaimStatusHistoryFinalise.aDescription"), lit("")).alias("clm_disp_type"),
            coalesce(col("dl_TIMClaimStatusHistoryFinalise.aCode"), lit("")).alias("clm_disp_type_cd"),
            to_date(col("dl_TIMPolicyClaim.aDateTimeEntered"), "yyyy-MM-dd").alias("clm_entrd_dt"),
            lit(None).cast(StringType()).alias("clm_flg"),
            coalesce(to_date(col("dl_TIMPolicyClaim.aDateClaimFormsReceived"), "yyyy-MM-dd"),
                     to_date(col("dl_TIMPolicyClaim.aDateNOACFirstReported"), "yyyy-MM-dd")).alias("clm_rpted_dt"),
            col("dl_TIMClaimStatus.aDescription").alias("clm_stat"),
            when(col("dl_TIMPolicyClaim.aCareType") == 1, "Yes").alias("clm_high_care_flg"),
            col("dl_TIMClaimStatus.aCode").alias("clm_stat_cd"),
            col("dl_CARClaimProfileCode.aDescription").alias("clm_type"),
            col("dl_TIMPolicyClaim.aLimitationExtendedBy").alias("clm_limitatn_extnded_by"),
            to_date(col("dl_TIMPolicyClaim.aDateNOACFirstReported"), "yyyy-MM-dd").alias("clm_noac_rpted_dt"),
            col("dl_TIMPolicyClaim.aNOACS39FollowUpStatus").alias("clm_noac_s39_1b_follow_up_stat"),
            col("dl_TIMPolicyClaim.aNominalDefendantPerc").cast(DecimalType(18, 2)).alias("clm_nominal_defendnt_pct"),
            col("dl_TIMPolicyClaim.aNominalDefendantReference").alias("clm_nominal_defendnt"),
            lit(None).cast(StringType()).alias("clm_company"),
            col("dl_TIMPolicyClaim.aIsNotableAndSignificantClaim").alias("clm_notable_significnt"),
            col("dl_TIMPolicyClaim.aIsNotableAndSignificantClaim").alias("clm_sensitive_rs"),
            coalesce(to_date(col("dl_TIMPolicyClaim.aDateClaimFormsReceived"), "yyyy-MM-dd"),
                     to_date(col("dl_TIMPolicyClaim.aDateNOACFirstReported"), "yyyy-MM-dd")).alias(
                "clm_form_notice_of_claim_rcvd_dt"),
            to_date(col("dl_TIMPolicyClaim.aDateFirstFinalised"), "yyyy-MM-dd").alias("clm_first_fnlised_dt"),
            to_date(col("dl_TIMPolicyClaim.aClaimStatusDate"), "yyyy-MM-dd").alias("clm_status_dt"),
            to_date(col("dl_TIMPolicyClaim.aDateReopened"), "yyyy-MM-dd").alias("clm_reopened_dt"),
            col("dl_TIMPolicyClaim.aIsUnreasonableContact").alias("clm_unreas_con"),
            to_date(col("dl_TIMPolicyClaim.aDateNOACFirstResponse"), "yyyy-MM-dd").alias(
                "clm_first_non_complnce_qld_s39_dt"),
            col("dl_TIMPolicyClaim.aANFAtFault").alias("clm_acc_fault_notifcn_form"),
            col("dl_CARClaimProfileCode.aDescription").alias("clm_cls"),
            coalesce(to_date(col("dl_TIMPolicyClaim.aDateClaimSatisfied"), "yyyy-MM-dd"),
                     to_date(col("dl_TIMPolicyClaim.aDateNOACSatisfied"), "yyyy-MM-dd"),
                     to_date(col("dl_TIMPolicyClaim.aDateS110"), "yyyy-MM-dd")).alias("clm_complnce_dt"),
            when(col("dl_TIMPolicyClaim.aNOACFirstResponseCode") == "01", "Accepted").when(
                col("dl_TIMPolicyClaim.aNOACFirstResponseCode") == "02",
                "Rejected").when(
                col("dl_TIMPolicyClaim.aNOACFirstResponseCode") == "03", "Waived").when(
                col("dl_TIMPolicyClaim.aNOACFirstResponseCode") == "04",
                "Not Required").alias(
                "clm_first_resp_noac_complnce_cd"),
            to_date(col("dl_TIMPolicyClaim.aDateNOACFirstResponse"), "yyyy-MM-dd").alias(
                "clm_first_resp_noac_complnce_dt"),
            col("dl_TIMLiabilityRejectReason.aDescription").alias("clm_subj_to_procedural_issue"),
            to_date(col("dl_TIMPolicyClaim.aWorkcoverSOCRECDate"), "yyyy-MM-dd").alias("clm_wc_recov_dt"),
            to_date(col("dl_TIMPolicyClaim.aDateLastContClaimant"), "yyyy-MM-dd").alias("clm_last_con_clmnt_dt"),
            lit(None).cast(StringType()).alias("clm_days_in_lifecyc_stg"),
            lit(None).cast(StringType()).alias("clm_demonstratable"),
            lit(None).cast(StringType()).alias("clm_lifecycle_stg"),
            lit(None).cast(StringType()).alias("clm_maic_rpted_flg"),
            col("dl_TIMPolicyClaim.aS84AHardshipPayment").alias("clm_fincl_hardshp_paymt_req_amt"),
            to_date(col("dl_TIMPolicyClaim.aS84AHardshipRequestDate"), "yyyy-MM-dd").alias(
                "clm_fincl_hardshp_paymt_req_dt"),
            lit(None).cast(StringType()).alias("clm_fnlisation_ay_grping"),
            to_date(col("dl_TIMPolicyClaim.aDateNOACSatisfied"), "yyyy-MM-dd").alias("clm_noac_satisfied_dt"),
            lit(None).cast(StringType()).alias("clm_null"),
            lit(None).cast(StringType()).alias("clm_online_form_id"),
            col("dl_TIMPolicyClaim.aInterimPayments").alias("clm_interim_pymt"),
            to_date(col("dl_TIMPolicyClaim.aLateClaimAcceptedDate"), "yyyy-MM-dd").alias(
                "clm_latst_of_accptnce_of_clm_subj_to_procedrl_issu_dt"),
            col("dl_TIMPolicyClaim.aClaimFormEntryAssistantName").alias("clm_persn_assistng_icf_completn"),
            when(lower(col("dl_CARClaimProfileCode.aDescription")) == "managed", col("dl_TIMState.aName"))
            .when(lower(col("dl_CARClaimProfileCode.aDescription")) == "interstate sa managed", "SA")
            .when(lower(col("dl_CARClaimProfileCode.aDescription")) == "interstate qld managed", "QLD")
            .when(lower(col("dl_CARClaimProfileCode.aDescription")) == "interstate nsw managed", "NSW")
            .otherwise(None).cast(StringType()).alias("clm_mangng_state"),  # Needs logic for accident state
            lit(None).cast(StringType()).alias("clm_minimal_impact_flg"),
            col("dl_TIMPolicyClaim.aNominalDefendantReference").alias("clm_nominal_defendnt_ref_no"),
            lit(None).cast(StringType()).alias("clm_null_rs"),
            lit(None).cast(StringType()).alias("clm_nulled_dt"),
            to_date(col("dl_TIMPolicyClaim.aDatePrescribedAuthoritySigned"), "yyyy-MM-dd").alias(
                "clm_prescrbd_auth_signd_dt"),
            when(col("dl_TIMPolicyClaim.aIsPrescribedAuthority") == 1, "Yes").alias("clm_prescrbd_auth_valid"),
            to_date(col("dl_TIMPolicyClaim.aDatePrescribedAuthorityValidTo"), "yyyy-MM-dd").alias(
                "clm_prescrbd_auth_valid_to_dt"),
            col("dl_TIMPolicyClaim.aClaimFormEntryAssistanceReason").alias("clm_rs_icf_completn_assistnce_reqrd"),
            lit(None).cast(StringType()).alias("clm_recov_by_anoth_insur"),
            col("dl_TIMClaimStatusHistoryReopen.aCode").alias("clm_reopen_rs_pir_cd"),
            col("dl_TIMClaimStatusHistoryReopen.aDescription").alias("clm_reopen_rs"),
            lit(None).cast(StringType()).alias("clm_rturn_to_wrk_full_dt"),
            lit(None).cast(StringType()).alias("clm_rturn_to_wrk_partial_dt"),
            to_date(col("dl_TIMPolicyClaim.aSatisfactoryAIFRecvDate"), "yyyy-MM-dd").alias(
                "clm_satisfactry_aif_rcvd_dt"),
            lit(None).cast(StringType()).alias("clm_event_cd"),
            to_date(col("dl_TIMPolicyClaim.aDateClaimFormsReceived"), "yyyy-MM-dd").alias("clm_s74_rpted"),
            lit("Insurer").alias("clm_ContactType"),
            when(col("dl_TIMPolicyClaim.aWorkcoverRecovery") == 1, "Yes").when(
                col("dl_TIMPolicyClaim.aWorkcoverRecovery") == 2, "No").when(
                col("dl_TIMPolicyClaim.aWorkcoverRecovery") == 3, "Pure").alias("clm_wc_recov"),
            # Source system identifier - ALWAYS REQUIRED
            lit(source).alias("source_system_cd")
        )

        return transformed_df

    def transform(self, source: str, ds: Dict[str, DataFrame]) -> DataFrame:
        """Transform source data to target schema with transformations in code.

        Args:
            source (str): The source system type
            ds (Dict[str, DataFrame]): Dictionary of loaded DataFrames

        Returns:
            DataFrame: The transformed data
        """
        self.log.info(self.__class__.__name__, "Implementing transformations in code")

        base_df_key = "dl_TIMPolicyClaim"

        if base_df_key not in ds:
            self.log.error(self.__class__.__name__,
                           f"Required base dataframe {base_df_key} not found in loaded datasets")
            sys.exit(1)

        final_data = self.return_final_df_after_joining_tables(ds, base_df_key)

        self.log.info(self.__class__.__name__, "Applying field transformations in code")

        final_data = self.return_selected_columns_from_final_dataframe(final_data, source)

        self.log.info(self.__class__.__name__, "Transformations completed successfully")
        return final_data
