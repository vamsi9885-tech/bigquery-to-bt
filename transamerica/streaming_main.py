####################################### DEVELOPMENT LOG ################################################################
# DESCRIPTION - Streaming main is used for calling the drivers for call streaming task
# USAGE 1: This Job Is used to call all streaming task
# 08/26/2025 - Ravikant Petkar - TLP-7685- added new policy_40_main_eft_nrt task and notification index
########################################################################################################################
import argparse
import logging
from src.streaming_apps.stream_to_datalake.driver_methods.horizon_driver import HorizonDriver
from src.streaming_apps.stream_to_datalake.driver_methods.pplus_driver import PplusDriver
from src.streaming_apps.stream_to_datalake.driver_methods.pds_driver import PdsDriver
from src.common_utils.custom_logging import CustomLogging
from src.streaming_apps.stream_to_datalake.driver_methods.phd_driver import PhdDriver
from src.streaming_apps.stream_to_datalake.driver_methods.dssp7_driver import Dssp7Driver
from src.streaming_apps.stream_to_datalake.driver_methods.mras_driver import MrasDriver
from builtins import str, Exception
import sys
from src.streaming_apps.stream_to_datalake.driver_methods.pds_dataload import PdsDataLoad
from src.streaming_apps.stream_to_datalake.driver_methods.pplus_dataload import PplusDataLoad
from src.streaming_apps.stream_to_datalake.driver_methods.hzn_dataload import HznDataLoad
from src.streaming_apps.stream_to_datalake.driver_methods.phd_batch_dataload import PhdDataLoadCustom

from src.streaming_apps.stream_to_datalake.driver_methods.awd_dataload import AwdDataLoad
from src.streaming_apps.stream_to_dynamodb.driver_function.pds_policy_customer_driver import PDSCustDriver
from src.streaming_apps.stream_to_dynamodb.driver_function.pds_policy_requirement_driver import PDSReqDriver
from src.streaming_apps.stream_to_dynamodb.driver_function.pds_policy_main_pending_driver import PDSMainPendingDriver
from src.streaming_apps.stream_to_dynamodb.driver_function.pds_policy_status_tracker_driver import AgentPolicyStatusTracker
from src.streaming_apps.stream_to_dynamodb.driver_function.pds_policy_inforce_60_benficiary_nrt_model_driver import AgentPolicyInforce60Benficiary
from src.streaming_apps.stream_to_dynamodb.driver_function.policy_40_main_nrt_driver import Policy40MainNrtDriver
from src.streaming_apps.stream_to_dynamodb.driver_function.policy_45_customer_nrt_driver import AgentPolicyInforce45Customer
from src.streaming_apps.stream_to_dynamodb.driver_function.policy_80_riders_nrt_driver import Policy80nrtDriver
from src.streaming_apps.stream_to_dynamodb.driver_function.policy_85_financial_nrt_driver import Policy85NRTDriver
from src.streaming_apps.stream_to_dynamodb.driver_function.policy_90_activity_nrt_driver import Policy90NRTDriver
from src.streaming_apps.stream_to_dynamodb.driver_function.policy_70_driver import Policy70Driver

class SdkClientException(Exception):
    """Raised when SdkClientException occurs"""
    pass


class ConcurrentModificationException(Exception):
    """Raised when ConcurrentModificationException occurs"""
    pass


class ResourceNotFoundException(Exception):
    """Raised when ResourceNotFoundException occurs"""
    pass


class UnknownHostException(Exception):
    """Raised when UnknownHostException occurs"""
    pass


class StreamingQueryException(Exception):
    """Raised when StreamingQueryException occurs"""
    pass


if __name__ == "__main__":
    CustomLogging.init()
    parser = argparse.ArgumentParser()
    parser.add_argument('--model', help="Provide model to run task. Required Field")
    parser.add_argument('--env', required=False,
                        help="Provide Provide secret ID. Required Field, possible values ['dev', 'tst', 'mdl', 'prd'] ")
    parser.add_argument('--load_type', required=False,
                        help="Provide Provide secret ID. Required Field, possible values ['FULL','DELTA','DELETE'] ")
    parser.add_argument('--es_vpc', required=False, help="Needed for OpenSearch Job.")
    parser.add_argument('--opensearch_index_name', required=False, help="Needed for BoB Jobs.")
    parser.add_argument('--opensearch_index_notification_name', required=False, help="Needed for BoB Jobs.")
    args = parser.parse_args()
    attempts = 1
    load_type = args.load_type
    print(f"🔍 VALIDATION: args.model = '{args.model}'")
    print(f"🔍 VALIDATION: args.model.upper() = '{args.model.upper()}'")
    print(f"🔍 VALIDATION: load_type = '{load_type}'")
    logging.info(f"🔍 VALIDATION: args.model = '{args.model}', load_type = '{load_type}'")
    print(args.model)
    print(load_type)
    job_run_flag = True
    # Handle PDS_CUSTOM_DELETE operation
    if args.model.upper() == "PDS_CUSTOM_DELETE":
        print("🚨 EMERGENCY DELETE ENTRY POINT ACTIVATED 🚨")
        print(f"Model: {args.model}")
        print(f"Load Type: {load_type}")
        logging.info("🚨 EMERGENCY DELETE ENTRY POINT ACTIVATED 🚨")
        logging.info(f"🔍 DEBUG: Model={args.model}, Load_Type={load_type}, Env={args.env}")
        
        try:
            print("🔄 ATTEMPTING IMPORT OF PdsOracleDeleteOperations")
            logging.info("🔄 ATTEMPTING IMPORT OF PdsOracleDeleteOperations")
            from src.streaming_apps.stream_to_datalake.driver_methods.pds_oracle_operations import PdsOracleDeleteOperations
            from pyspark.sql import SparkSession
            print("✅ IMPORT SUCCESSFUL")
            logging.info("✅ IMPORT SUCCESSFUL")
            
            print("🔄 CREATING SPARK SESSION")
            logging.info("🔄 CREATING SPARK SESSION")
            spark = SparkSession.builder \
                .appName("PDS_DELETE_OPERATION") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .config("spark.sql.hive.convertMetastoreParquet", "false") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog") \
                .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension") \
                .enableHiveSupport() \
                .getOrCreate()
            print("✅ SPARK SESSION CREATED")
            logging.info("✅ SPARK SESSION CREATED")
            
            print("🔄 INITIALIZING DELETE OPERATIONS")
            logging.info("🔄 INITIALIZING DELETE OPERATIONS")
            dboperations = PdsOracleDeleteOperations(spark, args.env.lower())
            print("✅ DELETE OPERATIONS INITIALIZED")
            logging.info("✅ DELETE OPERATIONS INITIALIZED")
            
            print("🔥 CALLING DELETE OPERATION FOR PLCY_CLOB ONLY 🔥")
            logging.info("🔥 CALLING DELETE OPERATION FOR PLCY_CLOB ONLY 🔥")
            dboperations.delete_records_from_datalake()
            print("✅ DELETE COMPLETED SUCCESSFULLY ✅")
            logging.info("✅ DELETE COMPLETED SUCCESSFULLY ✅")
            
        except Exception as e:
            print(f"❌ PDS_CUSTOM_DELETE FAILED: {str(e)}")
            logging.error(f"❌ PDS_CUSTOM_DELETE FAILED: {str(e)}")
            import traceback
            print(f"❌ TRACEBACK: {traceback.format_exc()}")
            logging.error(f"❌ TRACEBACK: {traceback.format_exc()}")
            raise e
            
        sys.exit(0)
    
    while job_run_flag:
        try:
            if args.model.upper() == "HORIZON":
                logging.info("Horizon kinesis to lake job starting.......")
                hzn = HorizonDriver(args.env.lower(), args.model.upper())
                hzn.driver_function()
            if args.model.upper() == "MRAS":
                logging.info("Mras kinesis to lake job starting.......")
                mras = MrasDriver(args.env.lower(), args.model.upper())
                mras.driver_function()
            if args.model.upper() == "PPLUS":
                pplus = PplusDriver(args.env.lower(), args.model.upper())
                pplus.driver_function()
            if args.model.upper() == "PDS":
                pds = PdsDriver(args.env.lower(), args.model.upper())
                pds.driver_function()
            if args.model.upper() == "PHD":
                phd = PhdDriver(args.env.lower(), args.model.upper())
                phd.driver_function()
            if args.model.upper() == "DSSP7":
                dssp7 = Dssp7Driver(args.env.lower(), args.model.upper())
                dssp7.driver_function()
            if args.model.upper() == "HZN_ORACLE":
                logging.info("Entering HZN data load main")
                print("Entering HZN data load main")
                hzn_main = HznDataLoad()
                hzn_main.secret_id = 'ta-individual-apps-{0}-common'.format(args.env.lower())
                hzn_main.env = args.env
                hzn_main.deltaLoad = 'DELTA'
                hzn_main.loadType = 'DELTA'
                print(hzn_main.loadType)
                hzn_main.hzn_load_main()
                attempts = 0

            if args.model.upper() == "PDS_DELETE_DIRECT":
                logging.info("🔥🔥🔥 DIRECT DELETE OPERATION - BYPASSING OLD CODE 🔥🔥🔥")
                print("🔥🔥🔥 DIRECT DELETE OPERATION - BYPASSING OLD CODE 🔥🔥🔥")
                job_run_flag = False
                from src.streaming_apps.stream_to_datalake.driver_methods.pds_oracle_operations import PdsOracleDeleteOperations
                from pyspark.sql import SparkSession
                spark = SparkSession.builder.appName("PDS_DELETE_DIRECT").enableHiveSupport().getOrCreate()
                dboperations = PdsOracleDeleteOperations(spark, args.env.lower())
                dboperations.delete_records_from_datalake()
                attempts = 0

            if args.model.upper() == "PDS_ORACLE":
                # EMERGENCY REDIRECT: If DELETE operation, use new delete handler
                if load_type and load_type.upper() == "DELETE":
                    logging.info("🔄 REDIRECTING PDS_ORACLE DELETE TO NEW HANDLER")
                    print("🔄 REDIRECTING PDS_ORACLE DELETE TO NEW HANDLER")
                    from src.streaming_apps.stream_to_datalake.driver_methods.pds_oracle_operations import PdsOracleDeleteOperations
                    from pyspark.sql import SparkSession
                    spark = SparkSession.builder.appName("PDS_ORACLE_DELETE_REDIRECT").enableHiveSupport().getOrCreate()
                    dboperations = PdsOracleDeleteOperations(spark, args.env.lower())
                    dboperations.delete_records_from_datalake()
                    attempts = 0
                else:
                    logging.info("Entering PDS data load main")
                    print("Entering PDS data load main")
                    pds_main = PdsDataLoad()
                    pds_main.deltaLoad = 'DELTA'
                    pds_main.secret_id = 'ta-individual-apps-{0}-common'.format(args.env.lower())
                    pds_main.env = args.env
                    pds_main.loadType = 'DELTA'
                    print(pds_main.loadType)
                    pds_main.pds_load_main()
                    attempts = 0

            if args.model == "policy_90_activity_nrt":
                policy_90_driver_obj = Policy90NRTDriver(args.env, args.model)
                logging.info("Calling Policy 90 driver function")
                job_run_flag = False
                policy_90_driver_obj.policy_90_activity_nrt_driver(args.es_vpc,args.opensearch_index_notification_name)

            if args.model == "policy_85_financial_nrt":
                policy_85_driver_obj = Policy85NRTDriver(args.env, args.model)
                logging.info("Calling Policy 85 driver function")
                job_run_flag = False
                policy_85_driver_obj.policy_85_financial_nrt_driver()

            if args.model == "policy_80_riders_nrt":
                policy_80_driver_obj = Policy80nrtDriver(args.env, args.model)
                logging.info("Calling Policy 80 driver function for streaming")
                job_run_flag = False
                policy_80_driver_obj.policy_80_riders_nrt_driver()

            if args.model == "policy_inforce_45_customer_nrt":
                pds_policy_inf_obj = AgentPolicyInforce45Customer(args.env, args.model)
                logging.info("Calling PDS policy_inforce_45_customer nrt driver function")
                job_run_flag = False
                pds_policy_inf_obj.agent_policy_inforce_45_customer_driver_function()

            if args.model == "policy_40_main_nrt":
                policy_40_main_driver_obj = Policy40MainNrtDriver(args.env, args.model)
                logging.info("Calling Policy 40 main function for streaming")
                job_run_flag = False
                policy_40_main_driver_obj.policy_40_main_nrt_driver(args.es_vpc,args.opensearch_index_name)

            if args.model == "policy_40_main_eft_nrt":
                policy_40_main_driver_obj = Policy40MainNrtDriver(args.env, args.model)
                logging.info("Calling Policy 40 main and EFT function for streaming")
                job_run_flag = False
                policy_40_main_driver_obj.policy_40_main_EFT_nrt_driver(args.es_vpc,args.opensearch_index_notification_name)

            if args.model == "policy_inforce_60_benficiary_nrt":
                pds_policy_inf_obj = AgentPolicyInforce60Benficiary(args.env, args.model)
                logging.info("Calling PDS policy_inforce_60_benficiary_tracker driver function")
                job_run_flag = False
                pds_policy_inf_obj.agent_policy_inforce_60_benficiary_driver_function()

            if args.model == "pds_policy_customer":
                pds_policy_customer_driver_obj = PDSCustDriver(args.env, args.model)
                logging.info("Calling PDS Policy Customer driver function")
                job_run_flag = False
                pds_policy_customer_driver_obj.pds_customer_driver_function()

            if args.model == "pds_policy_requirement":
                pds_policy_requirement_driver_obj = PDSReqDriver(args.env, args.model)
                logging.info("Calling PDS Policy Requirement driver function")
                job_run_flag = False
                pds_policy_requirement_driver_obj.pds_requirement_driver_function()

            if args.model == "pds_policy_main_pending":
                pds_policy_main_pending_driver_obj = PDSMainPendingDriver(args.env, args.model)
                logging.info("Calling PDS Policy Main Pending driver function")
                job_run_flag = False
                pds_policy_main_pending_driver_obj.pds_main_pending_driver_function(args.es_vpc,args.opensearch_index_name)

            if args.model == "policy_agent_status_tracker":
                pds_policy_agent_status_tracker_driver_obj = AgentPolicyStatusTracker(args.env, args.model)
                logging.info("Calling  PDS Policy Agent Status Tracker driver function")
                job_run_flag = False
                pds_policy_agent_status_tracker_driver_obj.agent_policy_status_tracker_driver_function()

            if args.model.upper() == "AWD":
                job_run_flag = False
                logging.info("Entering AWD data load main. LOAD TYPE : {0}".format(load_type))
                awd_main = AwdDataLoad()
                awd_main.secret_id = 'ta-individual-apps-{0}-common'.format(args.env.lower())
                awd_main.env = args.env
                awd_main.awd_load_main(load_type)

            if args.model == "policy_70":
                job_run_flag = False
                policy_70_driver_obj = Policy70Driver(args.env, args.model)
                logging.info("Calling Policy 70 driver function")
                policy_70_driver_obj.policy_70_driver_function()

            if args.model.upper() == "PDS_ORACLE_FULL_LOAD":
                logging.info("Entering PDS data load main")
                print("Entering PDS data load main")
                job_run_flag = False
                pds_main = PdsDataLoad()
                pds_main.secret_id = 'ta-individual-apps-{0}-common'.format(args.env.lower())
                pds_main.env = args.env
                pds_main.loadType = 'FULL'
                print(pds_main.loadType)
                pds_main.pds_load_main()

            if args.model.upper() == "PPLUS_DB2_LOAD":
                logging.info("Entering PPLUS data load main")
                print("Entering PPLUS data load main")
                job_run_flag = False
                pplus_main = PplusDataLoad()
                pplus_main.secret_id = 'ta-individual-apps-{0}-common'.format(args.env.lower())
                pplus_main.env = args.env
                pplus_main.loadType = load_type
                print(pplus_main.loadType)
                pplus_main.pplus_load_main()

            if args.model.upper() == "PPLUS_DB2_CUSTOM":
                logging.info("Entering PPLUS data load main")
                print("Entering PPLUS data load main")
                pplus_main = PplusDataLoad()
                pplus_main.secret_id = 'ta-individual-apps-{0}-common'.format(args.env.lower())
                pplus_main.env = args.env
                pplus_main.loadType = 'DELTA'
                pplus_main.deltaLoad = 'DELTA'
                print(pplus_main.loadType)
                pplus_main.pplus_load_main()
                attempts = 0

            if args.model.upper() == "PDS_ORACLE_CLOB":
                logging.info("Entering PDS data load main")
                print("Entering PDS data load main")
                job_run_flag = False
                pds_main = PdsDataLoad()
                pds_main.deltaLoad = 'DELTA'
                pds_main.secret_id = 'ta-individual-apps-{0}-common'.format(args.env.lower())
                pds_main.env = args.env
                pds_main.appType = args.model.upper()
                pds_main.loadType = load_type
                print(pds_main.loadType)
                pds_main.pds_load_main()
                attempts = 0

            if args.model.upper() == "PDS_ORACLE_INFORCE":
                logging.info("Entering PDS data load main")
                print("Entering PDS data load main")
                job_run_flag = False
                pds_main = PdsDataLoad()
                pds_main.deltaLoad = 'DELTA'
                pds_main.secret_id = 'ta-individual-apps-{0}-common'.format(args.env.lower())
                pds_main.env = args.env
                pds_main.appType = args.model.upper()
                pds_main.loadType = load_type
                print(pds_main.loadType)
                pds_main.pds_load_main()
                attempts = 0

            if args.model.upper() == "PDS_ORACLE_CLOB_DELETE":
                logging.info("Entering PDS CLOB delete operation")
                job_run_flag = False
                pds_main = PdsDataLoad()
                pds_main.secret_id = 'ta-individual-apps-{0}-common'.format(args.env.lower())
                pds_main.env = args.env
                pds_main.appType = 'PDS_ORACLE_CLOB'
                pds_main.loadType = 'DELETE'
                pds_main.deltaLoad = 'DELETE'
                pds_main.table_name = 'PLCY_CLOB'
                pds_main.pds_load_main()
                logging.info("PDS CLOB delete operation completed")
                attempts = 0




            if args.model.upper() == "HZN_ORACLE_LOAD":
                logging.info("Entering HZN data load main")
                print("Entering HZN data load main")
                job_run_flag = False
                hzn_main = HznDataLoad()
                hzn_main.secret_id = 'ta-individual-apps-{0}-common'.format(args.env.lower())
                hzn_main.env = args.env
                hzn_main.loadType = load_type
                print(hzn_main.loadType)
                hzn_main.hzn_load_main()
            
            if args.model.upper() == "PHD_CUSTOM_LOAD":
                logging.info("Entering PHD data load main")
                print("Entering PHD data load main")
                if load_type.upper() == 'DELTA':
                    job_run_flag = True
                else:
                    job_run_flag = False
                phd_main = PhdDataLoadCustom()
                phd_main.secret_id = 'ta-individual-apps-{0}-common'.format(args.env.lower())
                phd_main.env = args.env
                phd_main.loadType = load_type
                print(phd_main.loadType)
                phd_main.phd_load_main()
                attempts = 0
            if attempts == 20:
                print('maximum number of attempts completed')
                sys.exit()
            attempts += 1
        except SdkClientException:
            print("SdkClientException OCCURED... Rtrying the job")
            logging.info("SdkClientException OCCURED... Rtrying the job")
        except ConcurrentModificationException:
            print("ConcurrentModificationException OCCURED... Rtrying the job")
            logging.info("ConcurrentModificationException OCCURED... Rtrying the job")
        except ResourceNotFoundException:
            print("ResourceNotFoundException OCCURED... Rtrying the job")
            logging.info("ResourceNotFoundException OCCURED... Rtrying the job")
        except UnknownHostException:
            print("UnknownHostException OCCURED... Rtrying the job")
            logging.info("UnknownHostException OCCURED... Rtrying the job")
        except StreamingQueryException:
            print("StreamingQueryException OCCURED... Rtrying the job")
            logging.info("StreamingQueryException OCCURED... Rtrying the job")
        except Exception as e:
            print("StreamingQueryException OCCURED... Rtrying the job")
            logging.info("StreamingQueryException OCCURED... Rtrying the job")
            print(e)
            logging.info(e)
        finally:
            if attempts == 20:
                raise Exception
        try :    
            if args.model.upper() == "HZN_ORACLE_LOAD_DOWN_DEPENDENT":
                logging.info("Entering HZN data load main")
                print("Entering HZN data load main")
                job_run_flag = False
                hzn_main = HznDataLoad()
                hzn_main.secret_id = 'ta-individual-apps-{0}-common'.format(args.env.lower())
                hzn_main.env = args.env
                hzn_main.loadType = load_type
                print(hzn_main.loadType)
                hzn_main.hzn_load_main(args.model.upper())

            if args.model.upper() == "HZN_ORACLE_LOAD_DOWN_NON_DEPENDENT":
                logging.info("Entering HZN data load main")
                print("Entering HZN data load main")
                job_run_flag = False
                hzn_main = HznDataLoad()
                hzn_main.secret_id = 'ta-individual-apps-{0}-common'.format(args.env.lower())
                hzn_main.env = args.env
                hzn_main.loadType = load_type
                print(hzn_main.loadType)
                hzn_main.hzn_load_main(args.model.upper())
            if args.model.upper() == "PPLUS_DB2_CUSTOM_DOWN_NON_DEPENDENT":
                logging.info("Entering PPLUS data load main")
                print("Entering PPLUS data load main")
                job_run_flag = False
                pplus_main = PplusDataLoad()
                pplus_main.secret_id = 'ta-individual-apps-{0}-common'.format(args.env.lower())
                pplus_main.env = args.env
                pplus_main.loadType = load_type
                print(pplus_main.loadType)
                pplus_main.pplus_load_main(args.model.upper())

            if args.model.upper() == "PPLUS_DB2_CUSTOM_DOWN_DEPENDENT":
                logging.info("Entering PPLUS data load main")
                print("Entering PPLUS data load main")
                job_run_flag = False
                pplus_main = PplusDataLoad()
                pplus_main.secret_id = 'ta-individual-apps-{0}-common'.format(args.env.lower())
                pplus_main.env = args.env
                pplus_main.loadType = load_type
                print(pplus_main.loadType)
                pplus_main.pplus_load_main(args.model.upper())
        except:
            raise Exception
