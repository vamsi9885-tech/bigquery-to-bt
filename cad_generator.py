import uuid
import json
import logging
import os, sys
module_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(module_dir)
from utils import Utils


class CadenceGen:
    def __init__(self, function_name):
        with open(f'{function_name}/function.json','r') as json_file:
            function_config = json.load(json_file)
        keyvault_name = function_config.get('keyvault_name', None)
        postgres_conn_string_secret_name = function_config.get('postgres_conn_string_secret_name', None)
        self.utils_obj = Utils(keyvault_name, None, postgres_conn_string_secret_name)

    def check_and_generate_cadence(self, feed_name, feed_startdate, feed_extraction_type):
        logging.info('check_and_generate_cadence function app is triggered')
        try:
            cadence = self.check_for_cadence(feed_name, feed_startdate, feed_extraction_type)
            logging.info("cadence Details",extra={"properties":{'cadence': cadence, 'feed_name': feed_name, 'feed_startdate': feed_startdate, 'feed_extraction_type': feed_extraction_type}})
            if not cadence :
                return {'Error': 'No matching cadence found, check the start date in config'}
            window_start_date = cadence[0][1]
            window_end_date = cadence[0][2]
            if cadence[0][0] is None:
                cadence_id = self.generate_cadence(feed_name, window_start_date, window_end_date, feed_extraction_type)
            else:
                cadence_id = cadence[0][0]
                file_arrival_status_data = self.get_filearrivalstatus_data(cadence_id)
                file_master_data = self.get_filemaster_data(cadence_id)
                for i in file_arrival_status_data:
                    if i not in file_master_data:
                        self.insert_filemaster(cadence_id, i[0],i[1],i[2],i[3])

            logging.info("The check_and_generate_cadence function executed successfully.")
            cadence_values = {
                'CadenceId': str(cadence_id),
                'CadenceStartDate': str(window_start_date),
                'CadenceEndDate': str(window_end_date)
            }
            return cadence_values
        except Exception:
            logging.exception("The check_and_generate_cadence function has failed.", exc_info=True)

    def get_filearrivalstatus_data(self, cadence_id):
        return self.utils_obj.run_select_query(f"""select file_name_format, logical_file_name, extraction_type, part 
            from file_arrival_status 
            where cadence_id = '{cadence_id}' """)

    def get_filemaster_data(self, cadence_id):
        return self.utils_obj.run_select_query(f"""select file_name_format, logical_file_name, extraction_type, part 
            from file_master 
            where cadence_id = '{cadence_id}' """)

    def check_for_cadence(self, feed_name, feed_startdate, feed_extraction_type):
        return self.utils_obj.run_select_query(f"""select distinct cadence_id, window_start_date, window_end_date, extraction_type 
            from file_arrival_status where feed_name = '{feed_name}' and extraction_type = '{feed_extraction_type}' 
            and ('{feed_startdate}' between window_start_date and window_end_date)""")

    def generate_cadence(self, feed_name, window_start_date, window_end_date, feed_extraction_type):
        logging.info("Generating cadence for feed_name: %s, window_start_date",extra={"properties":{'feed_name': feed_name, 'window_start_date': window_start_date, 'window_end_date': window_end_date, 'feed_extraction_type': feed_extraction_type}})

        self.utils_obj.run_insert_query(f"""insert into cadence_master (cadence_id, feed_name, feed_startdate, feed_enddate, feed_type, extraction_type) 
            SELECT md5(concat('{feed_name}','{feed_extraction_type}',replace('{window_start_date}','-',''))) as cadence_id,'{feed_name}','{window_start_date}','{window_end_date}'
            , feed_config->>'feed_type' as feed_type, '{feed_extraction_type}' 
            from client_feed_config where feed_name = '{feed_name}' """)

        self.utils_obj.run_insert_query(f"""insert into file_master (cadence_id, file_name_format, logical_file_name, extraction_type, part) 
            With extraction_details as (
                SELECT json_array_elements(feed_config->'extraction_details')::json as e
                FROM client_feed_config
                WHERE feed_name = '{feed_name}'
            )
            SELECT md5(concat('{feed_name}','{feed_extraction_type}',replace('{window_start_date}','-',''))) as cadence_id
                , e ->>'file_name_format' as file_name_format
                , e ->>'logical_file_name' as logical_file_name
                , '{feed_extraction_type}' 
                , generate_series(coalesce((e ->> 'part_start_seq')::int, 1), (coalesce((e ->> 'part_start_seq')::int, 1) - 1) + coalesce((e ->> 'part_count')::int, 1)) as part
            from extraction_details
        """)

        logging.info("Cadence generated successfully")

        cadence_id = self.utils_obj.run_select_query(f"""select cadence_id from cadence_master where feed_name = '{feed_name}' and 
        extraction_type = '{feed_extraction_type}' and feed_startdate = '{window_start_date}' and feed_enddate = '{window_end_date}'
        and feed_type = (select feed_config->>'feed_type' as feed_type from client_feed_config where feed_name = '{feed_name}' )""")
        
        logging.info(f"cadence_id generated from hash", extra={"properties":{'cadence_id': cadence_id, 'feed_name': feed_name, 'feed_startdate': window_start_date, 'feed_extraction_type': feed_extraction_type}})
        return cadence_id

    def insert_filemaster(self, cadence_id, file_name_format, logical_file_name, feed_extraction_type, part):
        logging.debug("Inserting missing filemaster record",extra={'cadence_id': cadence_id, 'file_name_format': file_name_format, 'logical_file_name': logical_file_name, 'feed_extraction_type': feed_extraction_type, 'part': part})
        self.utils_obj.run_insert_query(f"""insert into file_master (cadence_id, file_name_format, logical_file_name, extraction_type, part) 
            VALUES ('{cadence_id}', '{file_name_format}', '{logical_file_name}', '{feed_extraction_type}', '{part}')""")
        return logging.info("Missed filemaster record(s) inserted successfully")
