    def generate_cadence(self, feed_name, window_start_date, window_end_date, feed_extraction_type):
        logging.info("Generating cadence for feed_name: %s, window_start_date",extra={"properties":{'feed_name': feed_name, 'window_start_date': window_start_date, 'window_end_date': window_end_date, 'feed_extraction_type': feed_extraction_type}})

        self.utils_obj.run_insert_query(f"""insert into cadence_master (cadence_id, feed_name, feed_startdate, feed_enddate, feed_type, extraction_type) 
            SELECT md5(concat('{feed_name}','{feed_extraction_type}',replace(replace(replace('{window_start_date}','-',''),':',''),' ',''))) as cadence_id,'{feed_name}','{window_start_date}','{window_end_date}'
            , feed_config->>'feed_type' as feed_type, '{feed_extraction_type}' 
            from client_feed_config where feed_name = '{feed_name}' """)

        self.utils_obj.run_insert_query(f"""insert into file_master (cadence_id, file_name_format, logical_file_name, extraction_type, part) 
            With extraction_details as (
                SELECT json_array_elements(feed_config->'extraction_details')::json as e
                FROM client_feed_config
                WHERE feed_name = '{feed_name}'
            )
            SELECT md5(concat('{feed_name}','{feed_extraction_type}',replace(replace(replace('{window_start_date}','-',''),':',''),' ',''))) as cadence_id
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
