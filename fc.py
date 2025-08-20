import datetime
import uuid
from datetime import date
import logging
import json
import time
from azure.eventgrid import EventGridPublisherClient
from azure.identity import DefaultAzureCredential
import os, sys
module_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(module_dir)
from format_json import FormatJSON
from utils import Utils


class FeedCompletion:
    def __init__(self):
        with open('Send_Feed_Completion_Notification/function.json','r') as json_file:
            function_config = json.load(json_file)
        keyvault_name = function_config.get('keyvault_name', None)
        http_post_url_secret_name = function_config.get('http_post_url_secret_name', None)
        postgres_conn_string_secret_name = function_config.get('postgres_conn_string_secret_name', None)

        self.utils_obj = Utils(keyvault_name, http_post_url_secret_name, postgres_conn_string_secret_name)
        self.format_json_obj = FormatJSON()

        self.environment_name = function_config.get('environment_name', None)
        self.success_event = function_config.get('dap_success_event', None)
        self.function_app_name = os.environ['WEBSITE_SITE_NAME']
        self.location = function_config.get('location', '')
        self.namespace = function_config.get('namespace','eip')

    def trigger_feed_completion_function_app(self):
        logging.info('Send Feed Completion Notification function app is triggered')
        try:
            self.fetch_completed_feeds_and_send_mail()
            time.sleep(5)
            logging.info("The Send_Feed_Completion_Notification function executed successfully.")
        except Exception as e:
            logging.error("The Send_Feed_Completion_Notification function has failed." + str(e),exc_info=True)

    def fetch_completed_feeds_and_send_mail(self):
        feeds = self.get_distinct_records()
        logging.info("Total number of distinct feed records to process", extra={"properties": {"record_count": len(feeds)}})
        logging.info('distinct feed details Fetched for Feed completion process.', extra={"properties": {"feeds": feeds}})
        completed_feeds = 0 # Initialize counter for completed feeds
        total = len(feeds)
        for index, feed in enumerate(feeds, start=1):
            try:
                feed_name = feed[0]
                expectation_date = feed[1]
                cadence_completion_status_sent_date = feed[2]
                cutoff_date = feed[3]
                is_adhoc_run = feed[4]
                client_name = feed[5]
                feed_frequency = feed[6]
                extraction_type = feed[7]
                window_start_date = feed[8]
                logging.info(f"Processing feed {index}: {feed_name}, window_start_date: {window_start_date}", extra={"properties": {"feed_name": feed_name,"window_start_date": window_start_date,"current_index": index,"total_feeds": total}})
                feed_completion_enable_flag = self.get_feed_completion_enable_flag(feed_name)
                logging.debug("checking feed_completion_enable_flag.", extra={"properties": {"feed_name": feed_name,"client_name": client_name,"feed_completion_enable_flag": feed_completion_enable_flag[0][0]}})
                if feed_completion_enable_flag[0][0] is None or feed_completion_enable_flag[0][0].lower() != "true":
                    logging.info("Ignoring this feed as the feed completion notification flag is disabled ")
                    continue
                if is_adhoc_run is True :
                    logging.info("This is an Ad-hoc feed")
                    output_folder_from_fas = feed[9]
                    rows = self.get_adhoc_consolidated_filecount(feed_name, output_folder_from_fas, extraction_type)
                    logging.info("Adhoc consolidated File details", extra={"properties": {"feed_name": feed_name,"client_name": client_name,"rows": rows}})
                    all_file_count = rows[0][2]
                    cadence_id = rows[0][0] if rows[0][0] else "N/A"
                    logging.info("Ad-hoc consolidated file details", extra={"properties": {"feed_name": feed_name,"client_name": client_name,"all_file_count": all_file_count}})
                    processed_file_count = self.get_adhoc_processed_file_count(feed_name, output_folder_from_fas, extraction_type)[0][0]
                    processedfiles = self.get_adhoc_processed_files(feed_name, output_folder_from_fas, extraction_type) # cadence_id, feed_id, arrival_date, file_name
                else :
                    rows = self.get_filecount_in_distinct_record(feed_name, expectation_date, extraction_type)
                    logging.info("Consolidated File rows", extra={"properties": {"feed_name": feed_name,"client_name": client_name,"rows": rows}})
                    all_file_count = rows[0][2]
                    processed_file_count = self.get_processed_file_count(feed_name, expectation_date, extraction_type)[0][0]
                    processedfiles = self.get_processed_files(feed_name, expectation_date, extraction_type) # cadence_id, feed_id, arrival_date, file_name
                processed_feed_id = {}
                processed_feedid_with_arrival_date = []
                logging.debug("processedfiles", extra={"properties": {"feed_name": feed_name,"client_name": client_name,"processedfiles": processedfiles}})
                for row in processedfiles:
                    file_name = row[3]
                    logging.debug("processing file",extra={"properties": {"feed_name": feed_name, "client_name": client_name, "file_name": file_name}})
                    processed_feed_id[(row[1], row[2])] = (row[1], row[2])
                processed_feedid_with_arrival_date = list(processed_feed_id.values())
                logging.debug("processed_feedid_with_arrival_date", extra={"properties": {"processed_feedid_with_arrival_date": processed_feedid_with_arrival_date}})

                schema_validation_passed_count = 0
                for element in processed_feedid_with_arrival_date:
                    feed_id = element[0]
                    arrival_date = element[1]
                    schema_validation_result = self.get_schema_validation_result(feed_id,extraction_type)
                    logging.info("feed_id schema_validation_result", extra={"properties": {"feed_id": feed_id, "schema_validation_result": schema_validation_result}})
                    passed_count = self.get_schema_validation_passed_count(feed_id,extraction_type)
                    schema_validation_passed_count += passed_count[0][0]

                logging.info("schema_validation_passed_count", extra={"properties": {"feed_name":feed_name,"schema_validation_passed_count": schema_validation_passed_count,"all_file_count": all_file_count,"processed_file_count": processed_file_count}})
                recipients = self.utils_obj.get_recipient_email(feed_name)
                logging.debug("recipients", extra={"properties": {"recipients": recipients[0][0]}})
                if all_file_count == processed_file_count and schema_validation_passed_count >= all_file_count:
                    logging.info("if condition called")
                    completed_feeds += 1 #Increment completed feed count
                    feed_details_json = []
                    failed_files_list = []
                    # Get processed and schema_validation_passed records
                    for row in processed_feedid_with_arrival_date:
                        feedid = row[0]
                        arrival_date = row[1]
                        inner_json = f'{{"feed_id": "{feedid}", "arrival_date": "{arrival_date}", "failed_files": {failed_files_list}}}'
                        feed_details_json.append(json.loads(inner_json))
                    feed_details_string = json.dumps(feed_details_json, ensure_ascii=False)
                    logging.info("Complete feed_details_string", extra={"properties": {"feed_details_string": feed_details_string}})
                    # Get final cadence_id's to update DB
                    cadenceids = list(set(row[0] for row in processedfiles))
                    logging.debug("Completed cadenceids", extra={"properties": {"cadenceids": cadenceids}})
                    # Send email & Update feed_completion_status_sent_date in DB
                    for cadenceidstring in cadenceids:
                        self.update_feed_completion_details(cadenceidstring, True)
                    if cadence_completion_status_sent_date is not None:
                        logging.info("Already sent the incomplete notification, hence ignoring this. DB completion flag update is done")
                    else :
                        logging.info("DB update is done, sending out the completion mail and trigger silver pipeline")
                        self.send_completed_feed_email(feed_name, feed_details_string, expectation_date, recipients[0][0],extraction_type)
                        folder_name = self.get_foldername(feed_frequency, window_start_date, cadenceids[0])
                        logging.debug("folder_name", extra={"properties": {"folder_name": folder_name}})

                        self.publish_message_to_event_grid(client_name, feed_frequency, feed_name, folder_name, extraction_type)
                        for cadenceidstring in cadenceids:
                            self.update_notification_sent_status(cadenceidstring)
                else:
                    logging.info("else condition called")
                    if is_adhoc_run is True :
                        logging.info("Ignoring this feed since it is an Adhoc run")
                        continue
                    if cadence_completion_status_sent_date is not None:
                        logging.info("Ignoring this feed as the incompletion notification is already sent")
                        continue
                    if cutoff_date >= date.today():
                        logging.info("Ignoring this feed as the cutoff date is not crossed")
                        continue
                    failedfiles = []
                    missedfiles = []
                    missed_file_names = []
                    feed_details_json = []
                    
                    # Get processed but schema_validation_failed records
                    for row in processed_feedid_with_arrival_date:
                        feedid = row[0]
                        arrival_date = row[1]
                        failedfiles = []
                        schema_validation_failed_records = self.get_schema_validation_failed_records(feedid,extraction_type)
                        total_records = len(schema_validation_failed_records)
                        logging.debug("schema_validation_failed_records",  extra={"properties": {"feed_name": feed_name, "client_name": client_name,"schema_validation_failed_records": schema_validation_failed_records}})
                        for index, row in enumerate(schema_validation_failed_records, start=1):
                            file_name = row[0]
                            logging.info(f"Processing schema validation failed record {index} for file: {file_name}",extra={"properties": {
                                     "feed_name": feed_name,
                                     "client_name": client_name,
                                      "file_name": file_name,
                                      "current_index": index,
                                      "total_records": total_records
                                    }})
                            # logging.debug("file_name", extra={"properties": {"file_name": file_name}})
                            failed_file_records = self.get_failed_files(feedid, file_name,extraction_type) # file_name, cadence_id, feed_id, arrival_date
                            logging.debug("failed_file_records", extra={"properties": {"failed_file_records": failed_file_records}})
                            if(len(failed_file_records) != 0):
                                failedfiles.append(failed_file_records[0])
                        logging.debug("failedfiles", extra={"properties": {"failedfiles": failedfiles}})
                        failed_file_names = [row[0] for row in failedfiles]
                        failed_files_list = json.dumps(failed_file_names)
                        logging.info("failed_files_list", extra={"properties": {"feed_name":feed_name,"failed_files_list": failed_files_list}})
                        inner_json = f'{{"feed_id": "{feedid}", "arrival_date": "{arrival_date}", "failed_files": {failed_files_list}}}'
                        feed_details_json.append(json.loads(inner_json))
                    feed_details_string = json.dumps(feed_details_json, ensure_ascii=False)
                    logging.info("Incomplete feed_details_string", extra={"properties": {"feed_name":feed_name,"feed_details_string": feed_details_string}})

                    # Get missed file details
                    missedfiles = self.get_missed_files(feed_name, expectation_date, extraction_type) # file_name_format, cadence_id, part
                    logging.info("missedfiles", extra={"properties": {"feed_name":feed_name,"missedfiles": missedfiles}})
                    for row in missedfiles:
                        file_name_format = row[0]
                        cadence_id = row[1]
                        part = row[2]
                        max_parts = self.get_part_count(cadence_id, file_name_format)[0][0]
                        if(int(max_parts) > 1):
                            missed_file_with_parts = "{'" + file_name_format + "' , [" + str(part) + "/" + str(max_parts) + "]}"
                            missed_file_names.append(missed_file_with_parts)
                        else:
                            missed_file_names.append(file_name_format)

                    if len(missed_file_names) != 0:
                        is_missing = "true"
                    else:
                        is_missing = "false"
                    logging.info("missedfilename", extra={"properties": {"feed_name": feed_name,"client_name": client_name, "missed_file_names": missed_file_names}})

                    # Get final cadence_id's to update DB
                    cadence_id_string = processedfiles[0][0] if len(processedfiles) != 0 else missedfiles[0][1]
                    logging.info("Final cadence_id_string", extra={"properties": {"feed_name": feed_name, "client_name": client_name, "cadence_id_string": cadence_id_string}})
                    # Send email & Update feed_completion_status_sent_date in DB
                    self.update_feed_completion_details(cadence_id_string, False)
                    self.send_incomplete_feed_email(feed_name, missed_file_names, feed_details_string, expectation_date, is_missing, recipients[0][0], extraction_type)
                    self.update_notification_sent_status(cadence_id_string)
            except Exception as e:
                logging.error("Failed at fetch_completed_feeds_and_send_mail method with error: " + str(e), exc_info=True)
                continue
        logging.info("Total number of completed feeds", extra={"properties": {"completed_feed_count": completed_feeds}})

    def send_completed_feed_email(self, feedname, feed_details_string, expectation_date, recipients,extraction_type):
        try:
            logging.info("Function for completed feed is called", extra={"properties": {"feedname": feedname, "extraction_type": extraction_type}})
            subject = f"Feed Completion Status : Complete for feed name - " + feedname + ", extraction_type - " + extraction_type
            feed_json = self.format_feed_completion_status_json(feed_details_string, feedname, "true", "false", [], expectation_date)
            logging.info("Complete Feed JSON", extra={"properties": {"feed_name":feedname,"feed_json": feed_json}})
            body = """This is an autogenerated email to inform you that all the files have been received for the given feed. <br>""" + feed_json
            self.utils_obj.send_mail(subject, body, recipients,"feed Completion")
        except Exception as e:
            logging.error("Failed at send_completed_feed_email method with error: " + str(e), exc_info=True)

    def send_incomplete_feed_email(self, feedname, missedfilenames, feed_details_string, expectation_date, is_missing, recipients, extraction_type):
        try:
            logging.info("Function for incomplete feed is called", extra={"properties": {"feedname": feedname, "extraction_type": extraction_type,"missedfilenames": missedfilenames}})
            subject = f"Feed Completion Status : Incomplete for feed name - " + feedname + " extraction_type - " + extraction_type
            feed_json = self.format_feed_completion_status_json(feed_details_string, feedname, "false", is_missing, missedfilenames, expectation_date)
            logging.info("Incomplete Feed JSON", extra={"properties": {"feed_json": feed_json, "feed_name": feedname}})
            body = """This is an autogenerated email to inform you that we have received only partial files for the given feed. <br>""" + feed_json
            self.utils_obj.send_mail(subject, body, recipients,"feed Completion")
        except Exception as e:
            logging.error("Failed at send_incomplete_feed_email method with error: " + str(e), exc_info=True)

    def format_feed_completion_status_json(self, feed_details_string, feedname, iscomplete, ismissing, missed_files, expectation_date):
        try:
            missed_files_list = json.dumps(missed_files)
            json_string = f'{{"feed_details": {feed_details_string}, "feed_name": "{feedname}", "is_complete": {iscomplete}, "is_missing": {ismissing},  "missed_files": {missed_files_list}, "expectation_date": "{expectation_date}"}}'
            return self.format_json_obj.generate_json(json_string)
        except Exception as e:
            logging.error("Failed at format_feed_completion_status_json method with error: " + str(e), exc_info=True)
            
    # invoke post request to call silver pipleine
    def publish_message_to_event_grid(self, client_name, feed_frequency, feed_name, folder_name, extraction_type):
        try:
            logging.info("Triggering event Grid to call silver")
            event_payload   = {}
            client_short_name_result = self.get_client_short_name(feed_name)
            client_short_name = client_name
            if client_short_name_result is not None:
                client_short_name_result = client_short_name_result[0]
                if client_short_name_result is not None:
                    client_short_name = client_short_name_result[0]

            environmentname = self.environment_name
            personaname = self.environment_name
            clientname = client_name
            namespacetemp = self.namespace
            # Below if cluse for running the eventgrid from hsodev environmemnt.
            if self.environment_name == 'nonprod' :
                environmentname = 'hsodev'
                personaname = 'dev'
                clientname = 'hsodev'
                namespacetemp = 'eip'

            event_grid_url = f'https://{namespacetemp}-{client_short_name}-e2e-event-grid.{self.location.lower().replace(" ", "")}-1.eventgrid.azure.net/api/events'

            folder_path = '/mnt/data/' + environmentname + '/bronze/' + feed_name + '/polishedbronze/' + extraction_type + '/current' + '/' + folder_name + '/'

            event_payload['id'] = f'Status-event-{uuid.uuid4()}'
            event_payload['eventType'] = f'Status Event'
            event_payload['subject']= f'Status Update from {self.function_app_name} for feed: {feed_name}'
            event_payload['eventTime'] = datetime.datetime.utcnow().isoformat()
            event_payload['data'] = {}
            # In Nonprod environment Eventgrid runs on hsodev and hence client name should be hsodev.
            event_payload['data']['client_name'] = clientname
            event_payload['data']['feedname'] = feed_name
            event_payload['data']['status'] = 'completed'
            event_payload['data']['pipeline_name'] = self.function_app_name
            event_payload['data']['run_id'] = folder_name.split('_')[0]
            # event_payload['data']['level'] = 'pb'                                # will be taken from load definition
            event_payload['data']['success_event'] = self.success_event

            if extraction_type.lower() == 'sample' or extraction_type.lower() == 'historic':  
                event_payload['data']['load_type'] = extraction_type  
            else:  
                event_payload['data']['load_type'] = feed_frequency 
                
            # In Nonprod environment Eventgrid runs on hsodev and hence persona name is dev. allowed names are [dev,qa,stage,prod]
            event_payload['data']['persona'] = personaname
            event_payload['data']['output_path'] = folder_path
            event_payload['data']['extra_args'] = f'feedname={feed_name}'
            event_payload['dataVersion'] = '1.0'
            
            logging.info("Invoking event_grid_url with body", extra={"properties": {"event_payload": json.dumps(event_payload)}})
            #credentials = AzureKeyCredential('')
            credential = DefaultAzureCredential()
            event_grid_client = EventGridPublisherClient(event_grid_url, credential)
            response = event_grid_client.send([event_payload])
            logging.info("Event sent successfully", extra={"properties": {"response": response}})
            return response
        except Exception as e:
            logging.error("Failed at trigger_silver_pipeline method with error: " + str(e), exc_info=True)

    def get_foldername(self, feed_frequency, expectation_date, cadence_id):
        result = self.utils_obj.run_select_query(f"""SELECT output_folder_name FROM cadence_master WHERE cadence_id = '{cadence_id}'""")
         # Check if result is not None and contains data
        if result and result[0][0] is not None:
            return result[0][0]
        else:
            if feed_frequency == 'yearly':
                folder_name = str(expectation_date.year)
            elif feed_frequency == 'monthly':
                folder_name = str(expectation_date.year) + str(expectation_date.month).zfill(2)
            else:
                folder_name = str(str(expectation_date).replace('-',''))
            return folder_name

    def get_distinct_records(self):
        return self.utils_obj.run_select_query("""
            WITH combined_file_arrival_status AS (
                SELECT feed_name, expectation_date, window_start_date, cadence_id, cutoff_date,
                    is_adhoc_run, client_name, feed_frequency, extraction_type, output_folder 
                FROM file_arrival_status
                UNION ALL
                SELECT feed_name, expectation_date, window_start_date, cadence_id, cutoff_date,
                    is_adhoc_run, client_name, feed_frequency, extraction_type, output_folder 
                FROM adhoc_file_arrival_status
            )
            SELECT DISTINCT fas.feed_name,
                            COALESCE(fas.expectation_date, fas.window_start_date),
                            cm.cadence_completion_status_sent_date,
                            fas.cutoff_date,
                            fas.is_adhoc_run,
                            fas.client_name,
                            fas.feed_frequency,
                            fas.extraction_type,
                            fas.window_start_date,
                            fas.output_folder
            FROM combined_file_arrival_status fas
            JOIN cadence_master cm ON fas.cadence_id = cm.cadence_id
            WHERE cadence_completion_flag IS NULL""") 
    
    def get_filecount_in_distinct_record(self, feed_name, expectation_date, extraction_type):
        return self.utils_obj.run_select_query(f"""SELECT fas.feed_name, fas.expectation_date, COUNT(1)
            FROM file_arrival_status fas
            JOIN cadence_master cm on fas.cadence_id = cm.cadence_id
            WHERE fas.feed_name = '{feed_name}' AND fas.expectation_date = '{expectation_date}' AND fas.extraction_type = '{extraction_type}'
            GROUP BY fas.feed_name, fas.expectation_date""")

    def get_adhoc_consolidated_filecount(self, feed_name, output_folder, extraction_type):
        return self.utils_obj.run_select_query(f"""
            WITH combined_file_arrival_status AS (
                SELECT feed_name, window_start_date, cadence_id, extraction_type, output_folder
                FROM file_arrival_status
                UNION ALL
                SELECT feed_name, window_start_date, cadence_id, extraction_type, output_folder
                FROM adhoc_file_arrival_status
            )
            SELECT fas.feed_name,
                fas.output_folder,
                COUNT(1)
            FROM combined_file_arrival_status fas
            WHERE fas.feed_name = '{feed_name}'
            AND fas.output_folder = '{output_folder}'
            AND fas.extraction_type = '{extraction_type}'
            GROUP BY fas.feed_name, fas.output_folder""")

    def get_processed_file_count(self, feed_name, expectation_date, extraction_type):
        return self.utils_obj.run_select_query(f"""SELECT COUNT(1)
            FROM file_arrival_status fas
            JOIN cadence_master cm on fas.cadence_id = cm.cadence_id
            JOIN file_master fm on fas.cadence_id = fm.cadence_id AND fas.logical_file_name = fm.logical_file_name AND fas.part = fm.part
            WHERE fas.feed_name = '{feed_name}' AND fas.expectation_date = '{expectation_date}' AND fas.extraction_type = '{extraction_type}'
            AND fm.arrived_flag IS TRUE""")

    def get_adhoc_processed_file_count(self, feed_name, output_folder, extraction_type):
        return self.utils_obj.run_select_query(f""" 
            WITH combined_file_arrival_status AS (
                SELECT cadence_id, logical_file_name, part, feed_name, window_start_date, extraction_type, output_folder
                FROM file_arrival_status
                UNION ALL
                SELECT cadence_id, logical_file_name, part, feed_name, window_start_date, extraction_type, output_folder
                FROM adhoc_file_arrival_status
            )
            SELECT COUNT(1)
            FROM combined_file_arrival_status fas
            JOIN cadence_master cm ON fas.cadence_id = cm.cadence_id
            JOIN file_master fm ON fas.cadence_id = fm.cadence_id AND fas.logical_file_name = fm.logical_file_name AND fas.part = fm.part
            WHERE fas.feed_name = '{feed_name}'
            AND fas.output_folder = '{output_folder}'
            AND fas.extraction_type = '{extraction_type}'
            AND fm.arrived_flag IS TRUE """)

    def get_processed_files(self, feed_name, expectation_date, extraction_type):
        return self.utils_obj.run_select_query(f"""SELECT fas.cadence_id, fm.feed_id, fm.arrival_date, fm.file_name, cm.cadence_completion_status_sent_date
            FROM file_arrival_status fas
            JOIN cadence_master cm on fas.cadence_id = cm.cadence_id
            JOIN file_master fm on fas.cadence_id = fm.cadence_id AND fas.logical_file_name = fm.logical_file_name AND fas.part = fm.part
            WHERE fas.feed_name = '{feed_name}' AND fas.expectation_date = '{expectation_date}' AND fas.extraction_type = '{extraction_type}'
            AND fm.arrived_flag IS TRUE""")

    def get_adhoc_processed_files(self, feed_name, output_folder, extraction_type):
        return self.utils_obj.run_select_query(f""" 
            WITH combined_file_arrival_status AS (
                SELECT cadence_id, logical_file_name, part, feed_name, window_start_date, extraction_type, output_folder
                FROM file_arrival_status
                UNION ALL
                SELECT cadence_id, logical_file_name, part, feed_name, window_start_date, extraction_type, output_folder
                FROM adhoc_file_arrival_status
            )
            SELECT fas.cadence_id,
                fm.feed_id,
                fm.arrival_date,
                fm.file_name,
                cm.cadence_completion_status_sent_date
            FROM combined_file_arrival_status fas
            JOIN cadence_master cm ON fas.cadence_id = cm.cadence_id
            JOIN file_master fm ON fas.cadence_id = fm.cadence_id AND fas.logical_file_name = fm.logical_file_name AND fas.part = fm.part
            WHERE fas.feed_name = '{feed_name}'
            AND fas.output_folder = '{output_folder}'
            AND fas.extraction_type = '{extraction_type}'
            AND fm.arrived_flag IS TRUE""")

    def get_missed_files(self, feed_name, expectation_date, extraction_type):
        return self.utils_obj.run_select_query(f"""SELECT fas.file_name_format, fas.cadence_id, fm.part
            FROM file_arrival_status fas
            JOIN cadence_master cm on fas.cadence_id = cm.cadence_id
            JOIN file_master fm on fas.cadence_id = fm.cadence_id AND fas.logical_file_name = fm.logical_file_name AND fas.part = fm.part
            WHERE fas.feed_name = '{feed_name}' AND fas.expectation_date = '{expectation_date}' AND fas.extraction_type = '{extraction_type}'
            AND fm.arrived_flag IS NOT TRUE""")

    def get_failed_files(self, feed_id, file_name,extraction_type):
        return self.utils_obj.run_select_query(f"""SELECT fas.file_name, fas.cadence_id, fas.feed_id, fm.arrival_date
            FROM file_arrival_status fas
            JOIN cadence_master cm on fas.cadence_id = cm.cadence_id
            JOIN file_master fm on fas.cadence_id = fm.cadence_id AND fas.logical_file_name = fm.logical_file_name AND fas.part = fm.part
            WHERE fas.feed_id = '{feed_id}' AND fas.file_name = '{file_name}' AND fas.extraction_type = '{extraction_type}'
            AND fm.arrived_flag IS TRUE""")

    def get_schema_validation_result(self, feed_id,extraction_type):
        return self.utils_obj.run_select_query(f"""SELECT feed_id, file_name, is_type_casting_success
            FROM file_master
            WHERE feed_id = '{feed_id}' AND extraction_type = '{extraction_type}'""")

    def get_schema_validation_failed_records(self, feed_id,extraction_type):
        return self.utils_obj.run_select_query(f"""SELECT file_name 
            FROM file_master 
            WHERE feed_id = '{feed_id}' AND is_type_casting_success = false AND extraction_type = '{extraction_type}'""")

    def get_schema_validation_passed_count(self, feed_id,extraction_type):
        return self.utils_obj.run_select_query(f"""SELECT COUNT(1) 
            FROM file_master 
            WHERE feed_id = '{feed_id}' AND is_type_casting_success = true AND extraction_type = '{extraction_type}' """)

    def get_client_short_name(self, feed_name):
        return self.utils_obj.run_select_query(f"""SELECT client_short_name 
            FROM client_feed_config 
            WHERE feed_name = '{feed_name}'""")

    def get_feed_completion_enable_flag(self, feed_name):
        return self.utils_obj.run_select_query(f"""SELECT feed_config->'feed_completion_notification'->>'enable' 
            FROM client_feed_config 
            WHERE feed_name = '{feed_name}'""")

    def update_notification_sent_status(self, cadence_ids):
        try:
            query = f"""UPDATE cadence_master SET cadence_completion_status_sent_date = '{date.today()}' WHERE cadence_id = '{cadence_ids}'"""
            self.utils_obj.run_insert_query(query)
        except Exception as e:
            logging.error("Failed at update_notification_sent_status method with error: " + str(e), exc_info=True)

    def update_feed_completion_details(self, cadence_ids, completion_flag):
        try:
            query = f"""UPDATE cadence_master SET cadence_completion_flag = {completion_flag} WHERE cadence_id = '{cadence_ids}'"""
            self.utils_obj.run_insert_query(query)
        except Exception as e:
            logging.error("Failed at update_feed_completion_details method with error: " + str(e), exc_info=True)

    def get_part_count(self, cadence_id, file_name_format):
            return self.utils_obj.run_select_query(f"""SELECT max(part) from file_master where 
                cadence_id = '{cadence_id}' and file_name_format = '{file_name_format}'""")
