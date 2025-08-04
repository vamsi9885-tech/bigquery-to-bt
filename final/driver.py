#!/bin/python
#####################################################
# File Name: driver.py
# Type: Pyspark
# Purpose: To Trigger the Individual Hub layer script.
# Created: 21/07/2020
# Last Updated: 22/10/2021
# Author: CDP Team
#####################################################

import sys

sys.path.append("src.zip")
"""Importing Surrogate Key"""
from src.reference.natural_surrkey import *
"""Importing Agent"""
from src.agent.mstr_intermediary import *
from src.agent.hub_intermediary_hierarchy import *
from src.agent.hub_intermediary_hierarchy_derived import *
from src.agent.mstr_intermediary_attestation import *
from src.agent.hub_site_seller_account import *
from src.agent.hub_site_seller_review import *
from src.agent.hub_site_seller_contact import *
from src.agent.hub_site_seller_lead import *
from src.agent.hub_aged_debt import *
from src.agent.ailgen_dpi import *
from src.agent.mstr_intermediary_cm import *
from src.agent.mstr_vendor_sap import *
from src.agent.mstr_broker_hpf import *
from src.agent.mstr_underwriter_hpf import *
from src.agent.agent_premium_class_rate import *
from src.agent.hub_site_seller_sms import *
"""Importing Clients and Platinum IDs"""
from src.client.mstr_quote_client_elife import *
from src.client.mstr_quote_client_life import *
from src.client.mstr_quote_client_saf2 import *
from src.client.dp_platinum_id_register import *
from src.client.dp_platinum_client import *
from src.client.dp_client_prep import *
from src.client.dp_saf_client_prep import *
from src.client.mstr_policy_client import *
from src.client.mstr_policy_client_mdm import *
from src.client.hub_cev_record import *
from src.client.hub_cev_client_ref import *
from src.client.mstr_client_cm import *
from src.client.mstr_client_hpf import *
"""Importing Quote and Policy"""
from src.policy.mstr_policy import *
from src.policy.hub_policy_periodic_debts import *
from src.quote.mstr_quote_saf2 import *
from src.quote.mstr_quote_elife import *
from src.quote.mstr_quote_life import *
from src.quote.mstr_quote_sbd import *
"""Importing Risk"""
from src.risk.hub_quote_home_risk import *
from src.risk.mstr_quote_motor import *
from src.risk.hub_quote_motor_risk import *
from src.risk.hub_quote_elife_risk import *
from src.risk.hub_policy_home_risk import *
from src.risk.hub_policy_motor_risk import *
from src.risk.hub_policy_life_risk import *
from src.risk.hub_quote_ctp_risk import *
from src.risk.hub_policy_ctp_risk import *
from src.risk.hub_policy_sbd_risk import *
from src.risk.hub_policy_sbd_engineering_risk import *
from src.risk.hub_policy_sbd_property_risk import *
from src.risk.hub_policy_sbd_liability_risk import *
from src.risk.hub_policy_sbd_home_risk import *
from src.risk.hub_quote_sbd_situation_risk import *
"""Importing Premium"""
from src.premium.hub_policy_premium import *
from src.premium.hub_quote_premium_home import *
from src.premium.hub_quote_premium_motor import *
from src.premium.hub_quote_premium_elife import *
from src.premium.hub_quote_premium_life import *
from src.premium.hub_quote_premium_ctp import *
from src.premium.hub_earned_premium import *
from src.premium.hub_technical_premium import *
from src.premium.hub_refund_premium import *
from src.premium.hub_policy_premium_new import *
from src.premium.hub_statement_premium import *
"""Importing Claims"""
from src.claim.hub_claim_header import *
from src.claim.hub_claim_header_wsp_update import *
from src.misc.quotexml_parser import *
from src.claim.hub_claim_movement import *
from src.claim.hub_claim_transaction_pcs import *
from src.claim.hub_ecosan_polisy_reporting import *
"""Importing Preference"""
from src.preference.hub_event import *
from src.preference.hub_event_abs import *
from src.preference.hub_quote_consent import *
"""Importing plan data """
from src.reference.direct_effective_plan import *
"""Importing Compaction"""
from src.compaction.compaction import *
from src.marketing.hub_campaign_events import *
from src.marketing.hub_campaign_performance import *
from src.marketing.hub_campaign_forecast import *
from src.marketing.hub_customer_quarantine import *
from src.reference.mstr_product import *
"""Importing hr """
from src.hr.hr_load import *
from src.hr.mstr_allianz_employee import *
from src.hr.hub_ewfm_full_load import *
from src.hr.hub_wfm_emp_avail_superstate import *
from src.hr.mstr_wfm_emp_details import *
from src.hr.hub_wfm_emp_termination_reasons import *
from src.hr.mstr_allianz_employee_details import *
from src.hr.hub_employee_course_status import *
"""Importing Communation"""
from src.communication.mstr_complaint import *
from src.communication.survey_response import *
from src.utils.file_ingestion_check import *
from src.communication.hub_external_agency_complaint import *
from src.communication.error_complaint import *
from src.communication.mstr_complaint_activity import *
from src.communication.mstr_complaint_contact import *
from src.communication.mstr_complaint_product_category import *
from src.communication.mstr_complaint_task import *
from src.communication.mstr_complaint_rootcause import *
from src.communication.hub_complaint import *
from src.communication.hub_omnia_interaction import *
from src.communication.hub_call_log import *
from src.communication.hub_qualtrix_survey import *
from src.communication.mstr_complaint_category_option import *
from src.communication.hub_avaya import *
from src.communication.hub_verint_dim_load import *
from src.communication.hub_verint_full_load import *
from src.communication.hub_webchat import *
from src.communication.hub_verint_call_eval_scores import *
from src.communication.hub_verint_fact_survey import *
from src.communication.hub_verint_fact_survey_answer import *
from src.communication.hub_verint_sessions_month import *
from src.communication.hub_verint_evals_on_form_component import *                                                      


# from src.modules.backup.backup import *
"""Importing ACMS"""
from src.claim.hub_claim_assessment_motor import *
from src.claim.hub_claim_assess_home import *
from src.claim.hub_claim_dtls_motor import *
from src.claim.hub_claim_status import *
from src.claim.hub_claim_tasks import *
from src.claim.hub_claim_comms_acms import *
from src.claim.hub_claim_events import *
from src.claim.hub_claim_repair_home import *
from src.claim.hub_claim_repair_motor import *
from src.claim.hub_claim_payment import *
from src.claim.hub_claim_payment_polw import *
from src.claim.hub_claim_assess_extrnl import *
from src.claim.hub_claim_statistics  import *
from src.claim.hub_claim_flag_hist  import *
from src.claim.hub_claim_fnol  import *
from src.claim.hub_claim_servis  import *
from src.claim.hub_claim_pdw  import *
from src.claim.hub_ecosan_report_pcs import *
from src.claim.hub_claim_cci_life import *
from src.hr.hub_polisy_user import *
from src.claim.hub_claim_payment_summary import *
from src.claim.hub_claim_reserve_payments import *
from src.hr.hub_polisy_user_polw import *
from src.claim.hub_claim_payment_summary_polw import *
from src.claim.hub_claim_reserve_payments_polw import *
from src.claim.hub_claim_pending_requests import *
from src.claim.hub_claim_pending_payments import *
from src.claim.hub_claim_pending_payment_summary import *
from src.claim.hub_claim_exgratia import *
"""Importing PNR Servis"""
from src.claim.hub_pnr_servis import *

"""Importing Claim FNOL GDF"""
from src.claim.hub_claim_fnol_gdf import *

"""Importing FNOL EVENTS LOG"""
from src.claim.hub_fnol_events_log import *

"""Importing PNR GDF"""
from src.claim.hub_pnr_gdf import *

"""Importing REF"""
from src.reference.hub_aal_branch import *
from src.reference.hub_acms_team_profile import *
from src.reference.hub_apra_prem_class import *
from src.reference.hub_claim_type_desc import *
from src.reference.hub_reference_load import *
from src.reference.pricing_expense import *
from src.reference.hub_claim_type_ref import *

"""Importing Check load"""
from src.misc.check_load import *
from src.misc.xml_parser import *

"""Importing CTP CARA & POLISY"""
from src.claim.hub_cara_ctp_accidents import *
from src.claim.hub_cara_ctp_vehicle import *
from src.claim.gen_claim_surr_key import *
from src.claim.gen_accident_surr_key import *
from src.claim.hub_ctp_calendar import *
from src.claim.hub_cara_ctp_claimant import *
from src.claim.hub_cara_ctp_claimestimates import *
from src.claim.hub_cara_ctp_claims import *
from src.claim.hub_cara_ctp_commonlawsettlement import *
from src.claim.hub_cara_ctp_contacts import *
from src.claim.hub_cara_ctp_earningcapacity import *
from src.claim.hub_cara_ctp_entitlement import *
from src.claim.hub_cara_ctp_injury_strategy import *
from src.claim.hub_cara_ctp_payment import *
from src.claim.hub_cara_ctp_statbenlegal import *
from src.claim.hub_cara_ctp_workflow import *
from src.claim.hub_polisy_ctp_accidents import *
from src.claim.hub_polisy_ctp_vehicle import *
from src.claim.hub_polisy_ctp_commonlawsettlement import *
from src.claim.hub_polisy_ctp_claims import *
from src.claim.hub_polisy_ctp_statbenlegal import *
from src.claim.hub_polisy_ctp_task import *
from src.claim.hub_polisy_ctp_claimestimates import *

""" Importing TIO CTP NT"""
from src.claim.hub_tioctp_claims import *
from src.claim.hub_tioctp_vehicle import *
from src.claim.hub_tioctp_claimstatus import *
from src.claim.hub_tioctp_riskprofile import *
from src.claim.hub_tioctp_liabstatus import *
from src.claim.hub_tioctp_accidents import *
from src.claim.hub_tioctp_claimant import *
from src.claim.hub_tioctp_accident_participant import *
from src.claim.hub_tioctp_claimestimate import *
from src.claim.hub_tioctp_liability_benefit import *
from src.claim.hub_tioctp_claimpayment import *
from src.claim.hub_tioctp_injuryplan import *
from src.claim.hub_tioctp_event_diary import *
from src.claim.hub_tioctp_resolution_strategy_plan import *
from src.claim.hub_tioctp_resolution_strategy_plan_history_loader import *
from src.claim.hub_tioctp_claimant_history_loader import *
from src.claim.hub_tioctp_claims_history_loader import *
from src.claim.hub_tioctp_accidents_history_loader import *
from src.claim.hub_tioctp_accident_participant_history_loader import *
from src.claim.hub_tioctp_claimpayment_history_loader import *
from src.claim.hub_tioctp_claimestimate_history_loader import *
from src.claim.hub_tioctp_vehicle_history_loader import *
from src.claim.hub_tioctp_liability_benefit_history_loader import *
from src.claim.hub_tioctp_injuryplan_history_loader import *
from src.claim.hub_tioctp_event_diary_history_loader import *
from src.claim.hub_tioctp_riskprofile_history_loader import *
from src.claim.hub_tioctp_liabstatus_history_loader import *
from src.claim.hub_tioctp_workflowactivity import *
from src.claim.hub_tioctp_claimstatus_history_loader import *
from src.claim.hub_tioctp_workflowactivity_history_loader import *
from src.claim.hub_tioctp_recoveryaction import *
from src.claim.hub_tioctp_recoveryaction_history_loader import *
from src.claim.hub_tioctp_injurycodes import *
from src.claim.hub_tioctp_injurycodes_history_loader import *
from src.claim.hub_tioctp_claimloec import *
from src.claim.hub_tioctp_claimloec_history_loader import *
from src.claim.hub_tioctp_resolution_action_plan import *
from src.claim.hub_tioctp_resolution_action_plan_history_loader import *
from src.claim.hub_tioctp_claiminjurycodeselection import *
from src.claim.hub_tioctp_claiminjurycodeselection_history_loader import *
from src.premium.hub_policy_premium_installment import *
from src.claim.hub_tioctp_claimestimatestatus_history_loader import *
from src.claim.hub_tioctp_claimestimatestatus import *

"""Importing Json load"""
from src.misc.salesforce_jsonparser import *
'''Importing hub_claim_repair_motor lake snapshot'''
from src.utils.lake_snapshot import *

""" Importing for LTC project"""
from src.claim.ltc_program import hub_ltc_processor

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    requiredNamed = parser.add_argument_group('required named arguments')
    requiredNamed.add_argument('--env', help='Provide environment [DEV, QA, PRD, UAT]', required=True)
    requiredNamed.add_argument('--waiting_time', help='Provide waiting time in minuts', required=False)
    requiredNamed.add_argument('--conf_file',
                               help='Please provide connection configuration yaml file [/user/xyz/abc.yml]',
                               required=True)
    requiredNamed.add_argument('--type_of_load',
                               help='Please provide the module name to be executed. '
                                    'For example hub_client for executing the client, mstr_intermediary '
                                    'for executing the agent', required=True)
    requiredNamed.add_argument('--start_date',
                               help='Provide the start date in combination with end_date to load specific time frame.'
                                    "e.g. '2018-11-22 00:00:00'")
    requiredNamed.add_argument('--end_date',
                               help='Please provide the end date in combination with start_date to load specific data. '
                                    "e.g. '2018-11-22 23:59:00'")
    requiredNamed.add_argument('--storage_conf',
                               help='Please provide storage configuration yaml file [/user/xyz/abc.yml]',
                               required=True)
    parser.add_argument('--entity',
                               help='Please provide the entity name [BankAustralia, Petplan, generic_complaints etc]')
    parser.add_argument('--options', help="Please provide optional parameters in key-value pairs eg. batch_months:<mm>,batch_years:<yy>")

    args = parser.parse_args()
    env = args.env.upper()
    type_of_load = args.type_of_load

    s = create_spark_session("Driver")
    (sc, hc) = s.get_sc_hc()

    if args.storage_conf:
        storage_env_prop = {}
        with open(args.storage_conf, 'r') as con:
            try:
                storage_env_prop = yaml.safe_load(con)
            except yaml.YAMLError as exc:
                sys.exit("Error: loading YAML failed" + args.storage_conf)

        cloud_storage = next(iter(storage_env_prop))
        storage_prop = storage_env_prop.get(cloud_storage)

        if storage_prop and storage_env_prop.get(cloud_storage).get(env):
            storage_prop.update(storage_env_prop.get(cloud_storage).get(env))
        else:
            sys.exit("Error: storage properties configuration is missing required keys")
        storage_prop.update({"sc": sc, "hc": hc})
        storage_function = cloud_storage.capitalize() + "Storage"

        try:
            storage = locals().get(storage_function)(cloud_storage, storage_prop)
        except TypeError as e:
            sys.exit("Error: Root key of storage property is incorrect: "+str(e))

    properties = {}
    with open(args.conf_file, 'r') as con:
        try:
            properties = yaml.safe_load(con)
        except yaml.YAMLError as exc:
            sys.exit(1)

    if storage.storage_name:
        print("Storage name:", storage.storage_name)
        print("Datahub url:", storage.driver_url_datahub)
        properties = storage.append_storage_driver(properties,
                                                  find=storage_env_prop.get(cloud_storage).get("path_pattern"),
                                                  file_type="yaml", url_type=storage.driver_url_datahub)

    if properties.get(env):
        env_props = properties[env]
    else:
        env_props = properties[args.type_of_load.upper()][env]
    env_props['env'] = env
    env_props['waiting_time'] = args.waiting_time
    env_props['start_date'] = args.start_date
    env_props['end_date'] = args.end_date
    env_props['type_of_load'] = type_of_load
    env_props['entity'] = args.entity
    if args.options:
        for i in args.options.split(','):
             key = i.split(':')[0].strip()
             value = i.split(':')[1].strip()
             env_props[key] = value

    app_logger = Log4j(hc)
    loads = {
        'dp_platinum_id_register': dp_platinum_id_register,
        'dp_platinum_client': dp_platinum_client,
        'dp_client_prep': dp_client_prep,
        'dp_platinum_id_register_saf': dp_platinum_id_register,
        'dp_platinum_client_saf': dp_platinum_client,
        'dp_saf_client_prep': dp_saf_client_prep,
        'mstr_quote_client_saf2': mstr_quote_client_saf2,
        'mstr_quote_client_elife': mstr_quote_client_elife,
        'mstr_quote_client_life': mstr_quote_client_life,
        'hub_quote_premium_home': hub_quote_premium_home,
        'hub_quote_premium_elife': hub_quote_premium_elife,
        'hub_quote_premium_life': hub_quote_premium_life,
        'hub_quote_premium_motor': hub_quote_premium_motor,
        'hub_policy_premium': hub_policy_premium,
        'mstr_intermediary': mstr_intermediary,
        'mstr_policy': mstr_policy,
        'mstr_policy_wsp': mstr_policy,
        'mstr_quote_saf2': mstr_quote_saf2,
        'mstr_quote_elife': mstr_quote_elife,
        'mstr_quote_life': mstr_quote_life,
        'hub_quote_elife_risk': hub_quote_elife_risk,
        'mstr_surr_polisy_agent': mstr_surrkey,
        'mstr_surr_polisy_claim': mstr_surrkey,
        'mstr_surr_polisy_client': mstr_surrkey,
        'mstr_surr_polisy_policy': mstr_surrkey,
        'mstr_surr_saf': mstr_surrkey,
        'mstr_surr_elife': mstr_surrkey,
        'quotexml_parser_saf1': quotexml_parser,
        'quotexml_parser_saf2': quotexml_parser,
        'quotexml_parser_elife': quotexml_parser,
        'quotexml_parser_aura_elife': quotexml_parser,
        'quotexml_parser_sbd': quotexml_parser,
        'mstr_policy_client': mstr_policy_client,
        'hub_policy_home_risk': hub_policy_home_risk,
        'hub_quote_motor_risk': hub_quote_motor_risk,
        'mstr_quote_motor': mstr_quote_motor,
        'hub_policy_motor_risk': hub_policy_motor_risk,
        'mstr_quote_home': mstr_quote_home,
        'hub_quote_home_risk': hub_quote_home_risk,
        'hub_policy_life_risk': hub_policy_life_risk,
        'hub_claim_header': hub_claim_header,
        'hub_event': hub_event,
        'compaction': compaction,
        'hub_campaign_events': hub_campaign_events,
        'hub_campaign_performance': hub_campaign_performance,
        'mstr_product': mstr_product,
        'direct_effective_plan': direct_effective_plan,
        'mstr_mcx_team_skill': hr_load,
        'mstr_mcx_employee': hr_load,
        'mstr_complaint': mstr_complaint,
        'hub_responses': survey_response,
        'hub_answers': survey_response,
        'hub_case_management': survey_response,
        'hub_questions': survey_response,
        'hub_respondents': survey_response,
        'hub_surveys': survey_response,
        'hub_text_analytics': survey_response,
        'hub_invites': survey_response,
        'hub_claim_assess_home': hub_claim_assess_home,
        'hub_claim_assessment_motor': hub_claim_assessment_motor,
        'hub_claim_status': hub_claim_status,
        'hub_claim_dtls_motor': hub_claim_dtls_motor,
        'hub_claim_tasks': hub_claim_tasks,
        'hub_claim_comms_acms': hub_claim_comms_acms,
        'hub_claim_repair_home': hub_claim_repair_home,
        'hub_claim_repair_motor': hub_claim_repair_motor,
        'hub_claim_events': hub_claim_events,
        'hub_claim_payment': hub_claim_payment,
        'hub_claim_assess_extrnl': hub_claim_assess_extrnl,
        'file_ingestion_check': file_ingestion_check,
        'error_complaint':error_complaint,
        'hub_external_agency_complaint':hub_external_agency_complaint,
        'hub_quote_premium_ctp': hub_quote_premium_ctp,
        'hub_quote_ctp_risk': hub_quote_ctp_risk,
        'hub_policy_ctp_risk': hub_policy_ctp_risk,
        'hub_campaign_forecast': hub_campaign_forecast,
        'hub_policy_sbd_risk': hub_policy_sbd_risk,
        'mstr_policy_client_mdm': mstr_policy_client_mdm,
        'mstr_complaint_activity': mstr_complaint_activity,
        'mstr_complaint_contact': mstr_complaint_contact,
        'mstr_complaint_product_category': mstr_complaint_product_category,
        'mstr_complaint_task': mstr_complaint_task,
        'mstr_complaint_rootcause': mstr_complaint_rootcause,
        'hub_event_abs': hub_event_abs,
        'check_load': check_load,
        'hub_complaint': hub_complaint,
        'hub_pnr_servis': hub_pnr_servis,
        'hub_pnr_gdf': hub_pnr_gdf,
        'hub_claim_fnol_gdf': hub_claim_fnol_gdf,
        'hub_omnia_interaction': hub_omnia_interaction,
        'hub_call_log': hub_call_log,
        'hub_claim_statistics': hub_claim_statistics,
        'hub_claim_flag_hist': hub_claim_flag_hist,
        'hub_claim_fnol': hub_claim_fnol,
        'hub_qualtrix_survey': hub_qualtrix_survey,
        'hub_fnol_events_log': hub_fnol_events_log,
        'hub_claim_servis': hub_claim_servis,
        'hub_aal_branch': hub_aal_branch,
        'hub_acms_team_profile': hub_acms_team_profile,
        'hub_claim_pdw': hub_claim_pdw,
        'hub_ecosan_report_pcs': hub_ecosan_report_pcs,
        'hub_apra_prem_class': hub_apra_prem_class,
        'hub_claim_type_desc': hub_claim_type_desc,
        'hub_intermediary_hierarchy': hub_intermediary_hierarchy,
        'hub_intermediary_hierarchy_derived': hub_intermediary_hierarchy_derived,
        'hub_reference_load': hub_reference_load,
        'hub_claim_movement': hub_claim_movement,
        'hub_earned_premium': hub_earned_premium,
        'hub_technical_premium': hub_technical_premium,
        'mstr_intermediary_attestation': mstr_intermediary_attestation,
        'hub_site_seller_account': hub_site_seller_account,
        'hub_site_seller_review': hub_site_seller_review,
        'hub_site_seller_contact': hub_site_seller_contact,
        'hub_site_seller_lead': hub_site_seller_lead,
        'hub_aged_debt': hub_aged_debt,
        'dp_account': salesforce_jsonparser,
        'dp_user': salesforce_jsonparser,
        'dp_userrole': salesforce_jsonparser,
        'dp_review3pd': salesforce_jsonparser,
        'dp_contact': salesforce_jsonparser,
        'dp_lead': salesforce_jsonparser,
        'dp_sms_message': salesforce_jsonparser,
        'pricing_expense':pricing_expense,
        'hub_cev_record': hub_cev_record,
        'hub_cev_client_ref': hub_cev_client_ref,
        'dp_document': xml_parser,
        'hub_claim_contact_compass': hub_reference_load,
        'hub_claim_case_compass': hub_reference_load,
        'hub_claim_case_history_compass': hub_reference_load,
        'hub_claim_case_party_compass': hub_reference_load,
        'hub_claim_document_compass': hub_reference_load,
        'hub_claim_document_history_compass': hub_reference_load,
        'hub_claim_header_compass': hub_reference_load,
        'hub_claim_incident_compass': hub_reference_load,
        'hub_claim_incident_contract_compass': hub_reference_load,
        'hub_claim_payment_compass': hub_reference_load,
        'hub_claim_payment_preference_compass': hub_reference_load,
        'hub_claim_reserve_compass': hub_reference_load,
        'hub_claim_reserve_history_compass': hub_reference_load,
        'hub_claim_task_compass': hub_reference_load,
        'hub_claim_task_history_compass': hub_reference_load,
        'hub_client_address_compass': hub_reference_load,
        'mstr_address_compass': hub_reference_load,
        'mstr_client_compass': hub_reference_load,
        'mstr_policy_compass': hub_reference_load,
        'hub_claim_dp_document_compass': hub_reference_load,
        'fi_acct_hierarchy': hub_reference_load,
        'fi_region_hierarchy': hub_reference_load,
        'fi_region_postcode': hub_reference_load,
        'ailgen_dpi': ailgen_dpi,
        'hub_claim_enum_fields_document_compass': hub_reference_load,
        'mstr_policy_pcs': hub_reference_load,
        'hub_policy_premium_pcs': hub_reference_load,
        'mstr_policy_extension_pcs': hub_reference_load,
        'hub_claim_header_pcs': hub_reference_load,
		'mstr_complaint_category_option': mstr_complaint_category_option,
        'hub_quote_consent': hub_quote_consent,
        'hub_claim_header_wsp_update': hub_claim_header_wsp_update,
        'hub_claim_repair_motor_snap': lake_snapshot,
        'hub_config_entity': hub_reference_load,
        'mstr_broker_hpf': mstr_broker_hpf,
        'mstr_underwriter_hpf': mstr_underwriter_hpf,
        'mstr_intermediary_cm': mstr_intermediary_cm,
        'mstr_vendor_sap': mstr_vendor_sap,
        'hub_ecosan_ref_load': hub_reference_load,
        'mstr_client_cm': mstr_client_cm,
        'mstr_client_hpf': mstr_client_hpf,
        'hub_claim_transaction_pcs': hub_claim_transaction_pcs,
        'mstr_allianz_employee': mstr_allianz_employee,
        'com_product_ref': hub_reference_load,
        'com_category_ref': hub_reference_load,
        'com_outcome_ref': hub_reference_load,
        'com_outcome_priority_ref': hub_reference_load,
        'com_bu_ref': hub_reference_load,
        'com_gender_ref': hub_reference_load,
        'com_age_ref': hub_reference_load,
        'hub_alex_ref_load': hub_reference_load,
        'hub_cyclone_ref_load': hub_reference_load,
        'hub_policy_sbd_engineering_risk': hub_policy_sbd_engineering_risk,
        'hub_policy_sbd_property_risk': hub_policy_sbd_property_risk,
        'hub_policy_sbd_liability_risk': hub_policy_sbd_liability_risk,
        'hub_policy_sbd_home_risk': hub_policy_sbd_home_risk,
        'hub_ecosan_daily_status_polisy': hub_ecosan_polisy_reporting,
        'hub_ecosan_daily_timeout_polisy': hub_ecosan_polisy_reporting,
        'hub_ecosan_monthly_status_polisy': hub_ecosan_polisy_reporting,
		'hub_refund_premium': hub_refund_premium,
        'webchat_queue_lookup': hub_reference_load,
        'tio_quality_mapping': hub_reference_load,
        'skill_name_mapping': hub_reference_load,
        'bot_ip': hub_reference_load,
        'quality_mapping': hub_reference_load,
        'endorsement_mapping': hub_reference_load,
        'complaints_mapping': hub_reference_load,
        'cancellation_mapping': hub_reference_load,
        'attrition_reason': hub_reference_load,
        'calls_mapping': hub_reference_load,
        'vdn_manager_output': hub_reference_load,
        'pcs_exclusions': hub_reference_load,
        'rpt_dpi': hub_reference_load,
        'skill_manager_output': hub_reference_load,
        'team_skill_lookup': hub_reference_load,
        'rm_broker_mapping': hub_reference_load,
		'agent_premium_class_rate': agent_premium_class_rate,
        'quotexml_parser_sbd_archived': quotexml_parser,
        'hub_ctp_claims_reference' : hub_reference_load,
        'hub_claim_cci_life': hub_claim_cci_life,
        'lead_partner_mapping': hub_reference_load,
        'mstr_quote_sbd': mstr_quote_sbd,
        'hub_quote_sbd_situation_risk': hub_quote_sbd_situation_risk,
        'dealer_sourcecode_mapping': hub_reference_load,
        'hub_cara_ctp_accidents': hub_cara_ctp_accidents,
        'hub_cara_ctp_vehicle': hub_cara_ctp_vehicle,
        'ctp_clm_car_sur_key': ctp_clm_car_sur_key,
        'ctp_acc_car_sur_key': ctp_acc_car_sur_key,
        'hub_ctp_calendar': hub_ctp_calendar,
        'hub_cara_ctp_claimant': hub_cara_ctp_claimant,
        'hub_cara_ctp_claimestimates': hub_cara_ctp_claimestimates,
        'hub_cara_ctp_claims': hub_cara_ctp_claims,
        'hub_cara_ctp_commonlawsettlement': hub_cara_ctp_commonlawsettlement,
        'hub_cara_ctp_contacts': hub_cara_ctp_contacts,
        'hub_cara_ctp_earningcapacity': hub_cara_ctp_earningcapacity,
        'hub_cara_ctp_entitlement': hub_cara_ctp_entitlement,
        'hub_cara_ctp_injury_strategy': hub_cara_ctp_injury_strategy,
        'hub_cara_ctp_payment': hub_cara_ctp_payment,
        'hub_cara_ctp_statbenlegal': hub_cara_ctp_statbenlegal,
        'hub_cara_ctp_workflow': hub_cara_ctp_workflow,
        'hub_polisy_ctp_claims': hub_polisy_ctp_claims,
        'hub_polisy_ctp_commonlawsettlement': hub_polisy_ctp_commonlawsettlement,
        'hub_polisy_ctp_statbenlegal': hub_polisy_ctp_statbenlegal,
        'hub_polisy_ctp_accidents': hub_polisy_ctp_accidents,
        'hub_polisy_ctp_vehicle': hub_polisy_ctp_vehicle,
        'hub_avaya': hub_avaya,
        'hub_polisy_ctp_task': hub_polisy_ctp_task,
        'hub_customer_quarantine': hub_customer_quarantine,
        'hub_pay_type': hub_reference_load,
        'hub_polisy_ctp_claimestimates': hub_polisy_ctp_claimestimates,
        'hub_policy_premium_new': hub_policy_premium_new,
        'hub_ewfm_full_load': hub_ewfm_full_load,
        'hub_claim_payment_polw': hub_claim_payment_polw,
        'hub_wfm_emp_avail_superstate': hub_wfm_emp_avail_superstate,
        'mstr_wfm_emp_details': mstr_wfm_emp_details,
        'hub_wfm_emp_termination_reasons': hub_wfm_emp_termination_reasons,
        'hub_verint_full_load': hub_verint_full_load,
        'hub_verint_dim_load': hub_verint_dim_load,
        'hub_webchat': hub_webchat,
        'hub_verint_call_eval_scores': hub_verint_call_eval_scores,
        'hub_polisy_user': hub_polisy_user,
        'hub_claim_payment_summary': hub_claim_payment_summary,
        'hub_claim_reserve_payments': hub_claim_reserve_payments,
        'hub_polisy_user_polw': hub_polisy_user_polw,
        'hub_claim_payment_summary_polw': hub_claim_payment_summary_polw,
        'hub_claim_reserve_payments_polw': hub_claim_reserve_payments_polw,
        'hub_claim_pending_requests': hub_claim_pending_requests,
        'hub_claim_pending_payments': hub_claim_pending_payments,
        'hub_claim_pending_payment_summary': hub_claim_pending_payment_summary,
        'hub_verint_fact_survey': hub_verint_fact_survey,
        'hub_verint_fact_survey_answer': hub_verint_fact_survey_answer,
        'hub_verint_sessions_month': hub_verint_sessions_month,
        'hub_verint_evals_on_form_component' : hub_verint_evals_on_form_component,
        'hub_statement_premium': hub_statement_premium,
        'hub_policy_premium_installment': hub_policy_premium_installment,
        'hub_claim_type_ref': hub_claim_type_ref,
        'hub_auto_training_mapping': hub_reference_load,
        'hub_fi_training_mapping': hub_reference_load,
        'hub_employee_course_status': hub_employee_course_status,
        'mstr_allianz_employee_details': mstr_allianz_employee_details,
        'hub_tioctp_claims': hub_tioctp_claims,
        'hub_tioctp_riskprofile': hub_tioctp_riskprofile,
        'hub_tioctp_liabstatus': hub_tioctp_liabstatus,
        'hub_tioctp_claimstatus': hub_tioctp_claimstatus,
        'hub_tioctp_vehicle': hub_tioctp_vehicle,
        'hub_tioctp_accidents': hub_tioctp_accidents,
        'hub_tioctp_claimant': hub_tioctp_claimant,
        'hub_tioctp_accident_participant': hub_tioctp_accident_participant,
        'hub_tioctp_claimestimate': hub_tioctp_claimestimate,
        'hub_tioctp_claimpayment': hub_tioctp_claimpayment,
        'hub_tioctp_injuryplan': hub_tioctp_injuryplan,
        'hub_tioctp_event_diary': hub_tioctp_event_diary,
        'hub_tioctp_resolution_strategy_plan': hub_tioctp_resolution_strategy_plan,
        'hub_tioctp_resolution_strategy_plan_history_loader': hub_tioctp_resolution_strategy_plan_history_loader,
        'hub_tioctp_resolution_action_plan': hub_tioctp_resolution_action_plan,
        'hub_tioctp_resolution_action_plan_history_loader': hub_tioctp_resolution_action_plan_history_loader,
        'hub_tioctp_claimloec': hub_tioctp_claimloec,
        'hub_tioctp_claimloec_history_loader': hub_tioctp_claimloec_history_loader,
        'hub_tioctp_liability_benefit': hub_tioctp_liability_benefit,
        'hub_tioctp_claimant_history_loader': hub_tioctp_claimant_history_loader,
        'hub_tioctp_claims_history_loader': hub_tioctp_claims_history_loader,
        'hub_tioctp_accidents_history_loader': hub_tioctp_accidents_history_loader,
        'hub_tioctp_accident_participant_history_loader': hub_tioctp_accident_participant_history_loader,
        'hub_tioctp_claimpayment_history_loader': hub_tioctp_claimpayment_history_loader,
        'hub_tioctp_claimestimate_history_loader': hub_tioctp_claimestimate_history_loader,
        'hub_tioctp_vehicle_history_loader': hub_tioctp_vehicle_history_loader,
        'hub_tioctp_liability_benefit_history_loader': hub_tioctp_liability_benefit_history_loader,
        'hub_tioctp_injuryplan_history_loader': hub_tioctp_injuryplan_history_loader,
        'hub_tioctp_event_diary_history_loader': hub_tioctp_event_diary_history_loader,
        'hub_tioctp_riskprofile_history_loader': hub_tioctp_riskprofile_history_loader,
        'hub_tioctp_liabstatus_history_loader': hub_tioctp_liabstatus_history_loader,
        'hub_tioctp_workflowactivity': hub_tioctp_workflowactivity,
        'hub_tioctp_workflowactivity_history_loader': hub_tioctp_workflowactivity_history_loader,
        'hub_tioctp_recoveryaction': hub_tioctp_recoveryaction,
        'hub_tioctp_recoveryaction_history_loader': hub_tioctp_recoveryaction_history_loader,
        'hub_tioctp_claimstatus_history_loader': hub_tioctp_claimstatus_history_loader,
        'hub_tioctp_injurycodes': hub_tioctp_injurycodes,
        'hub_tioctp_injurycodes_history_loader': hub_tioctp_injurycodes_history_loader,
        'hub_tioctp_claiminjurycodeselection': hub_tioctp_claiminjurycodeselection,
        'hub_tioctp_claiminjurycodeselection_history_loader': hub_tioctp_claiminjurycodeselection_history_loader,
        'hub_policy_periodic_debts': hub_policy_periodic_debts,
        'hub_claim_exgratia': hub_claim_exgratia,
        'plan_data': hub_reference_load,
        'hub_site_seller_sms': hub_site_seller_sms,
        'hub_tioctp_claimestimatestatus_history_loader': hub_tioctp_claimestimatestatus_history_loader,
        'hub_tioctp_claimestimatestatus': hub_tioctp_claimestimatestatus,
        'hub_ltc_processor': hub_ltc_processor
    }
    app_logger.info(__name__, "Load_type" + type_of_load)
    loads[type_of_load](hc, env_props, app_logger)
    hc.stop()
