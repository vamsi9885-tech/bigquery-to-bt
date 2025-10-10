from datetime import date, datetime
import json
import logging
import os, sys
module_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(module_dir)
from format_email import FormatEmail
from cad_generator import CadenceGen
from utils import Utils
import pytz


class FAM:
    def __init__(self):
        with open('Send_FAM_Email/function.json','r') as json_file:
            function_config = json.load(json_file)
        keyvault_name = function_config.get('keyvault_name', None)
        http_post_url_secret_name = function_config.get('http_post_url_secret_name', None)
        postgres_conn_string_secret_name = function_config.get('postgres_conn_string_secret_name', None)
        self.env_name = function_config.get('environment_name','prod').upper()
        self.utils_obj = Utils(keyvault_name, http_post_url_secret_name, postgres_conn_string_secret_name)
        central_timezone = pytz.timezone('America/Chicago')
        self.current_central_time = datetime.now(central_timezone).replace(tzinfo=None)
        self.now = self.current_central_time.strftime('%Y-%m-%d %H:%M:%S')
        self.current_date = self.current_central_time.date()

    def trigger_fam_email_function_app(self):
        logging.info('Send_FAM_Email function app is triggered')
        try:
            logging.info('Calling Cadence Generator from FAM function app.')
            self.generate_cadence()
            logging.info('Executing FAM Function.')
            self.fetch_files_and_send_mail()
            logging.info("The Send_FAM_Email function executed successfully.")
        except Exception as e:
            logging.error("The Send_FAM_Email function has failed. %s", str(e), exc_info=True)

    def generate_cadence(self):
        cadence = self.get_records_without_cadence_id()
        if cadence != []:
            logging.debug("Cadence records count", extra={"properties": {"cadence_records_count": len(cadence)}})
            logging.debug("Cadence records fetched which not contains cadence_id", extra={"properties": {"cadence_records": cadence}})
            total = len(cadence)
            for index, cad in enumerate(cadence, start=1):
                try:
                    logging.info("Cadence record details", extra={"properties": {
                        "cad": cad,
                        "feed_name": cad[0],
                        "start_date": cad[1],
                        "extraction_type": cad[2],
                        "current_index": index,
                        "total_count": total
                    }})
                    feed_name = cad[0]
                    start_date = cad[1]
                    extraction_type = cad[2]
                    cad_gen = CadenceGen("Send_FAM_Email")
                    cadence_values = cad_gen.check_and_generate_cadence(feed_name, start_date, extraction_type)
                    logging.info("cadence-values", extra={"properties": {"cadence_values": cadence_values, "feed_name": feed_name, "start_date": start_date, "extraction_type": extraction_type}})
                except Exception as e:
                    logging.error("generate_cadence fuction has some exception : " + str(e), exc_info=True)
                    continue
        logging.info('Cadence Generator finished cadence generation')

    def fetch_files_and_send_mail(self):
        clientlist = self.get_client_names()
        logging.info("Client list fetched", extra={"properties": {"clientlist": clientlist, "client_count": len(clientlist)}})
        total_clients = len(clientlist)
        for i, client in enumerate(clientlist, start=1):
            try:
                client_name = client[0]
                logging.info(f"Current Processing client {i}: {client_name}",extra={"properties": {"client_name": client_name, "index": i, "total": total_clients}})
                cadence = self.get_file_arrival_status(client_name)
                cadence_with_updated_status = self.calculate_status_for_files(cadence)
                cadence_id_column_index = 0
                data = [  
                        (row[1], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11])  
                        for row in cadence_with_updated_status  
                    ] 
                feed_names_from_db = [row[3] for row in cadence_with_updated_status] 
                cadence_ids=[row[cadence_id_column_index] for row in cadence_with_updated_status]
                logging.info("Cadence details", extra={"properties": {"client_name": client_name,"cadence_with_updated_status": cadence_with_updated_status,"data": data}})

                recipients = self.get_recipient_email(client_name)
                logging.debug("recipients ", extra={"properties": {"client_name":client_name,"recipients":recipients[0][0]}})
                distinct_feed_name = set(feed_names_from_db)
                feed_wording = "Feeds" if len(distinct_feed_name) > 1 else "Feed"
                feed_names = ", ".join(distinct_feed_name)
                subject = f"File Arrival Monitor - Report on Late Files for Client ({client_name}) for {feed_wording} ({feed_names})"
                if self.env_name and self.env_name != "PROD":
                    subject = f"[{self.env_name}] {subject}"
                body = f"""This automated email serves as a notification that Optum has not received the following files. We will begin to review the situation from our end. Our team at Optum will investigate the matter, take any necessary action, and reach out to you for any additional details as required. If you have not yet provided the files listed below, we kindly request you to do so at your earliest convenience.
                """ + self.format_fam_email(data)
                if len(data) > 0:
                    output = self.utils_obj.send_mail(subject, body, recipients[0][0], "Send_FAM_Email")
                    logging.info("Successfully sent email", extra={"properties": {"client_name": client_name,"cadence_id": cadence}})

                row_count = 0
                for row in cadence_with_updated_status:
                    cadence_id = row[0]
                    status = row[11]
                    file_name_format = row[4]
                    part = cadence[row_count][5]
                    self.update_status_and_sentdate(cadence_id, status, file_name_format, part)
                    row_count += 1
                logging.debug("updated status in DB")
            except Exception as e:
                logging.error(str(e), exc_info=True)
                continue

    def get_records_without_cadence_id(self):
        now = self.now

        return self.utils_obj.run_select_query(f""" SELECT distinct feed_name, window_start_date, extraction_type 
            FROM file_arrival_status where  (expectation_date < '{now}')
            and (('{now}' BETWEEN DATE(yellow_email_start_date) AND DATE(yellow_email_end_date))
                OR ('{now}' BETWEEN DATE(red_email_start_date) AND DATE(red_email_end_date))) 
            and cadence_id is null group by feed_name, window_start_date, extraction_type""")

    def get_client_names(self):
        now = self.now
        return self.utils_obj.run_select_query(f"""SELECT distinct client_name FROM file_arrival_status f inner join file_master d on f.cadence_id = d.cadence_id and f.file_name_format = d.file_name_format  where d.arrived_flag is not true and f.expectation_date < '{now}' and (('{now}' BETWEEN f.yellow_email_start_date AND f.yellow_email_end_date) OR ('{now}' BETWEEN f.red_email_start_date AND f.red_email_end_date))""")

    def get_file_arrival_status(self, clientname):
        now = self.now
        current_date=self.current_date
        return self.utils_obj.run_select_query(f"""SELECT f.cadence_id, f.client_name, f.feed_id, f.feed_name, f.file_name_format, f.part, f.is_mandatory
            , f.feed_frequency, f.expectation_date, f.lag_tolerance, d.delay_reason, d.status, f.yellow_email_start_date, f.yellow_email_end_date
            , f.red_email_start_date, f.red_email_end_date
            FROM file_arrival_status f inner join file_master d on f.cadence_id = d.cadence_id and f.file_name_format = d.file_name_format and f.part = d.part
            inner join client_feed_config cfc on f.feed_name = cfc.feed_name
            where f.client_name = '{clientname}' and d.arrived_flag is not true and (expectation_date < '{now}')
            and (('{now}' BETWEEN f.yellow_email_start_date AND f.yellow_email_end_date) 
                OR ('{now}' BETWEEN f.red_email_start_date AND f.red_email_end_date)) and cfc.feed_config->>'is_active' = 'true' 
            and (d.notification_sent_date IS NULL OR DATE(d.notification_sent_date) != '{current_date}')
            order by f.expectation_date, f.file_name_format, f.part""")

    def calculate_status_for_files(self, data_with_cadence):
        updated_data_with_cadence = []
        for row in data_with_cadence:
            logging.info("Current proccessing", extra={"properties": {"cadence_id": row[0],"is_mandatory": row[6],}})
            is_mandatory = row[6]
            yellow_email_start_date = row[12]
            yellow_email_end_date = row[13]
            red_email_start_date = row[14]
            red_email_end_date = row[15]
            logging.debug("current_date", extra={"properties": {"current_date": str(self.current_date), "now": str(self.now)}})
            if(yellow_email_start_date is not None and yellow_email_end_date is not None and (yellow_email_start_date <= self.current_central_time <= yellow_email_end_date)):
                if is_mandatory:
                    updated_row = row[:11] + ("Late",) + row[12:]
                else:
                    updated_row = row[:11] + ("Warning",) + row[12:]
                updated_status = updated_row
            elif(red_email_start_date is not None and red_email_end_date is not None and (red_email_start_date <= self.current_central_time <= red_email_end_date)):
                if is_mandatory:
                    updated_row = row[:11] + ("Late and Exceeds Lag Time",) + row[12:]
                else:
                    updated_row = row[:11] + ("Warning and Exceeds Lag Time",) + row[12:]
                updated_status = updated_row
            else:
                updated_status = row

            cadence_id = updated_status[0]
            file_name_format = updated_status[4]
            part = updated_status[5]
            max_parts = self.get_part_count(cadence_id, file_name_format)[0][0]
            if(int(max_parts) > 1):
                updated_part = updated_status[:5] + (f"{part}/{max_parts}",) + updated_status[6:]
            else:
                updated_part = updated_status[:5] + ("N/A",) + updated_status[6:]

            updated_data_with_cadence.append(updated_part)
        return updated_data_with_cadence

    def update_status_and_sentdate(self, cadence_id, status, file_name_format, part):
        try:
            now = self.now
            query = f"""UPDATE file_master set status = '{status}', notification_sent_date = '{now}'
                where cadence_id = '{cadence_id}' and file_name_format = '{file_name_format}' and part = '{part}'"""
            self.utils_obj.run_insert_query(query)
        except Exception as e:
            logging.error(str(e), exc_info=True)

    def get_recipient_email(self, clientname):
        return self.utils_obj.run_select_query(f"""SELECT distinct CONCAT_WS(';',feed_config->'email_notification'->>'internal_ids', feed_config->'email_notification'->>'external_ids') 
            from client_feed_config 
            where client_name = '{clientname}' and (feed_config->'email_notification'->>'internal_ids' is not NULL or feed_config->'email_notification'->>'external_ids' is not NULL);""")

    def get_part_count(self, cadence_id, file_name_format):
        return self.utils_obj.run_select_query(f"""SELECT max(part) from file_master where 
            cadence_id = '{cadence_id}' and file_name_format = '{file_name_format}'""")

    def format_fam_email(self, data):
        try:
            headers = ["Client Name", "Feed Name", "Expected File Name", "Part File No.", "Required for Load?", "Feed Frequency", "Expected File Arrival Date", "Lag Tolerance", "Delay Reason", "Status"]
            format_email_obj = FormatEmail()
            return format_email_obj.generate_html_table_for_fam(headers, data)
        except Exception as e:
            logging.error(str(e), exc_info=True)
