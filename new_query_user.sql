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
                            fas.output_folder,
                            cfc.feed_config->'extraction_settings'->>'e2e_custom_load_type'
            FROM combined_file_arrival_status fas
            JOIN cadence_master cm ON fas.cadence_id = cm.cadence_id
            JOIN client_feed_config cfc ON fas.feed_name = cfc.feed_name
            WHERE cm.cadence_completion_flag IS NULL
            and cfc.feed_config->>'is_active' = 'true'""")


INSERT INTO public.client_feed_config (client_id, client_name, feed_name, created_time_stamp, created_by, modified_by, client_short_name, feed_config) VALUES ('regressionv1', 'regressionv1', 'new_notify_type_1',CURRENT_TIMESTAMP,'build','build', 'dev','{"client_name": "regressionv1", "client_id": "regressionv1", "feed_name": "new_notify_type_1", "connector_type": "`epsi", "connector_version": "v1.0", "feed_type": "push", "is_active": true, "frequency": {"type": "weekly", "rolling_data_days": 0}, "extraction_settings": {"row_delimiter": "\\r\\n", "column_delimiter": ",", "quote": "N", "storage_base_path": "../..", "data_cleansing": {}, "implementation_setting": {"is_adhoc_run": false, "extraction_type": "periodic", "split_by": "weekly", "adhoc_start_date": "2025-07-01", "adhoc_end_date": "2025-07-31", "periodic_start_date": "2025-08-04", "lag_offset": 4, "lag_tolerance": 3, "notify_type": "standard"}, "connection_setting": {"type": "push"}, "regex_date_group_number": 2}, "extraction_details": [{"file_name_format": "^.*(devices)_(\\d{8})_part_([0-9]+).*", "logical_file_name": "devices", "is_mandatory": true, "part_count": "10", "part_start_seq": 1}], "enable_run_status_notification": false, "feed_completion_notification": {"enable": true}, "email_notification": {"internal_ids": "alavala_babu@optum.com", "external_ids": "alavala_babu@optum.com"}, "preserve_archives": true, "purge_old_files_by_days": 30}') ON CONFLICT (feed_name) DO UPDATE SET client_id = EXCLUDED.client_id, client_name = EXCLUDED.client_name, created_time_stamp = EXCLUDED.created_time_stamp, created_by = EXCLUDED.created_by, modified_by = EXCLUDED.modified_by, feed_config = EXCLUDED.feed_config;



INSERT INTO public.client_feed_config (client_id, client_name, feed_name, created_time_stamp, created_by, modified_by, client_short_name, feed_config) VALUES ('regressionv1', 'regressionv1', 'new_UI_None_type',CURRENT_TIMESTAMP,'build','build', 'dev','{"client_name": "regressionv1", "client_id": "regressionv1", "feed_name": "new_UI_None_type", "connector_type": "epsi_push", "connector_version": "v1.0", "feed_type": "push", "is_active": true, "frequency": {"type": "quarterly", "rolling_data_days": 0}, "extraction_settings": {"row_delimiter": "\\r\\n", "column_delimiter": ",", "quote": "N", "storage_base_path": "../..", "data_cleansing": {}, "implementation_setting": {"is_adhoc_run": false, "extraction_type": "periodic", "split_by": "daily", "adhoc_start_date": "2025-08-01", "adhoc_end_date": "2025-08-04", "periodic_start_date": "2025-08-18", "lag_offset": 0, "lag_tolerance": 0, "notify_type": "standard"}, "connection_setting": {"type": "push"}, "regex_date_group_number": 2}, "extraction_details": [{"file_name_format": "^.*(devices)_(\\d{8})_part_([0-9]+).*", "logical_file_name": "devices", "is_mandatory": true, "part_count": "2", "part_start_seq": 1}], "enable_run_status_notification": false, "feed_completion_notification": {"enable": true}, "email_notification": {"internal_ids": "alavala_babu@optum.com", "external_ids": "alavala_babu@optum.com"}, "preserve_archives": true, "purge_old_files_by_days": 30}') ON CONFLICT (feed_name) DO UPDATE SET client_id = EXCLUDED.client_id, client_name = EXCLUDED.client_name, created_time_stamp = EXCLUDED.created_time_stamp, created_by = EXCLUDED.created_by, modified_by = EXCLUDED.modified_by, feed_config = EXCLUDED.feed_config;



INSERT INTO public.client_feed_config (client_id, client_name, feed_name, created_time_stamp, created_by, modified_by, client_short_name, feed_config) VALUES ('regressionv1', 'regressionv1', 'new_notify_type',CURRENT_TIMESTAMP,'build','build', 'dev','{"client_name": "regressionv1", "client_id": "regressionv1", "feed_name": "new_notify_type", "connector_type": "espi_push", "connector_version": "v1.0", "feed_type": "push", "is_active": true, "frequency": {"type": "daily", "rolling_data_days": 0}, "extraction_settings": {"row_delimiter": "\\r\\n", "column_delimiter": ",", "quote": "N", "storage_base_path": "../..", "data_cleansing": {}, "implementation_setting": {"is_adhoc_run": false, "extraction_type": "periodic", "split_by": "daily", "adhoc_start_date": "2025-07-01", "adhoc_end_date": "2025-07-31", "periodic_start_date": "2025-08-18", "lag_offset": 2, "lag_tolerance": 10, "notify_type": "standard"}, "connection_setting": {"type": "push"}, "regex_date_group_number": 2}, "extraction_details": [{"file_name_format": "^.*(devices)_(\\d{8})_([0-9]+).*", "logical_file_name": "devices", "is_mandatory": true, "part_count": "*", "part_start_seq": 1}], "enable_run_status_notification": false, "feed_completion_notification": {"enable": true}, "email_notification": {"internal_ids": "alavala_babu@optum.com", "external_ids": "alavala_babu@optum.com"}, "preserve_archives": true, "purge_old_files_by_days": 30}') ON CONFLICT (feed_name) DO UPDATE SET client_id = EXCLUDED.client_id, client_name = EXCLUDED.client_name, created_time_stamp = EXCLUDED.created_time_stamp, created_by = EXCLUDED.created_by, modified_by = EXCLUDED.modified_by, feed_config = EXCLUDED.feed_config;





INSERT INTO public.client_feed_config (client_id, client_name, feed_name, created_time_stamp, created_by, modified_by, client_short_name, feed_config) VALUES ('regressionv1', 'regressionv1', 'new_none_type',CURRENT_TIMESTAMP,'build','build', 'dev','{"client_name": "regressionv1", "client_id": "regressionv1", "feed_name": "new_none_type", "connector_type": "epsi_psuh", "connector_version": "v1.0", "feed_type": "push", "is_active": true, "frequency": {"type": "monthly", "rolling_data_days": 0}, "extraction_settings": {"row_delimiter": "\\r\\n", "column_delimiter": ",", "quote": "N", "storage_base_path": "../..", "data_cleansing": {}, "implementation_setting": {"is_adhoc_run": false, "extraction_type": "periodic", "split_by": "monthly", "adhoc_start_date": "2025-08-01", "adhoc_end_date": "2025-08-10", "periodic_start_date": "2025-08-14", "lag_offset": 5, "lag_tolerance": 10, "notify_type": "standard"}, "connection_setting": {"type": "push"}, "regex_date_group_number": 2}, "extraction_details": [{"file_name_format": "^.*(payers)_(\\d{8})_part_([0-9]+).*", "logical_file_name": "payers", "is_mandatory": true, "part_count": "*", "part_start_seq": 1}], "enable_run_status_notification": false, "feed_completion_notification": {"enable": true}, "email_notification": {"internal_ids": "alavala_babu@optum.com", "external_ids": "alavala_babu@optum.com"}, "preserve_archives": true, "purge_old_files_by_days": 30}') ON CONFLICT (feed_name) DO UPDATE SET client_id = EXCLUDED.client_id, client_name = EXCLUDED.client_name, created_time_stamp = EXCLUDED.created_time_stamp, created_by = EXCLUDED.created_by, modified_by = EXCLUDED.modified_by, feed_config = EXCLUDED.feed_config;


