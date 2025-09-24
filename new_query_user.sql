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





WITH combined_file_arrival_status AS (
    SELECT feed_name, expectation_date, window_start_date, cadence_id, cutoff_date,
           is_adhoc_run, client_name, feed_frequency, extraction_type, output_folder 
    FROM file_arrival_status
    UNION ALL
    SELECT feed_name, expectation_date, window_start_date, cadence_id, cutoff_date,
           is_adhoc_run, client_name, feed_frequency, extraction_type, output_folder 
    FROM adhoc_file_arrival_status
),
feed_parts AS (
    SELECT 
        feed_name,
        CASE 
            WHEN bool_or((ed->>'part_count') = '*') THEN '*'
            ELSE '1'
        END AS part_count_flag
    FROM client_feed_config cfc,
         jsonb_array_elements(cfc.feed_config->'extraction_details') ed
    GROUP BY feed_name
)
SELECT DISTINCT fas.feed_name,
       COALESCE(fas.expectation_date, fas.window_start_date) AS expectation_date,
       cm.cadence_completion_status_sent_date,
       fas.cutoff_date,
       fas.is_adhoc_run,
       fas.client_name,
       fas.feed_frequency,
       fas.extraction_type,
       fas.window_start_date,
       fas.output_folder,
       cfc.feed_config->'extraction_settings'->>'e2e_custom_load_type' AS e2e_custom_load_type,
       fp.part_count_flag
FROM combined_file_arrival_status fas
JOIN cadence_master cm ON fas.cadence_id = cm.cadence_id
JOIN client_feed_config cfc ON fas.feed_name = cfc.feed_name
JOIN feed_parts fp ON fas.feed_name = fp.feed_name
WHERE cm.cadence_completion_flag IS NULL
  AND cfc.feed_config->>'is_active' = 'true';






dev_dap_run_log=> select * from fas_cadence_config where client_name = 'regressionv1' order by feed_name;
  client_id   | client_name  |     feed_name     | feed_type | feed_frequency | window_cron_schedule | fam_cron_schedule | periodic_start_date | lag_offset | lag_tolerance | notify
_type | adhoc_start_date | adhoc_end_date | is_adhoc_run | adhoc_split_by | extraction_type |                                                                         file_details  
                                                                       | source 
--------------+--------------+-------------------+-----------+----------------+----------------------+-------------------+---------------------+------------+---------------+-------
------+------------------+----------------+--------------+----------------+-----------------+---------------------------------------------------------------------------------------
-----------------------------------------------------------------------+--------
 regressionv1 | regressionv1 | new_none_type     | push      | monthly        |                      |                   | 2025-08-14          |          5 |            10 | standa
rd    | 2025-08-01       | 2025-08-10     | f            | monthly        | periodic        | {"file_name_format": "^.*(devices)_(\\d{8})_part_([0-9]+).*", "logical_file_name": "de
vices", "is_mandatory": true, "part_count": "10", "part_start_seq": 1} | config
 regressionv1 | regressionv1 | new_notify_type   | push      | quarterly      |                      |                   | 2025-08-18          |          2 |            10 | standa
rd    | 2025-07-01       | 2025-07-31     | f            | quarterly      | periodic        | {"file_name_format": "^.*(devices)_(\\d{8})_part_([0-9]+).*", "logical_file_name": "de
vices", "is_mandatory": true, "part_count": "*", "part_start_seq": 1}  | config
 regressionv1 | regressionv1 | new_notify_type   | push      | quarterly      |                      |                   | 2025-08-18          |          2 |            10 | standa
rd    | 2025-07-01       | 2025-07-31     | f            | quarterly      | periodic        | {"file_name_format": "^.*(claims)_(\\d{8})_part_([0-9]+).*", "logical_file_name": "cla
ims", "is_mandatory": true, "part_count": "2", "part_start_seq": 1}    | config
 regressionv1 | regressionv1 | new_notify_type_1 | push      | weekly         |                      |                   | 2025-08-04          |          4 |             3 | standa
rd    | 2025-07-01       | 2025-07-31     | f            | weekly         | periodic        | {"file_name_format": "^.*(devices)_(\\d{8})_part_([0-9]+).*", "logical_file_name": "de
vices", "is_mandatory": true, "part_count": "*", "part_start_seq": 1}  | config
 regressionv1 | regressionv1 | new_UI_None_type  | push      | daily          |                      |                   | 2025-08-18          |          0 |             0 | standa
rd    | 2025-08-01       | 2025-08-04     | f            | daily          | periodic        | {"file_name_format": "^.*(payers)_(\\d{8})_part_([0-9]+).*", "logical_file_name": "pay
ers", "is_mandatory": true, "part_count": "1", "part_start_seq": 1}    | config
 regressionv1 | regressionv1 | new_UI_None_type  | push      | daily          |                      |                   | 2025-08-18          |          0 |             0 | standa
rd    | 2025-08-01       | 2025-08-04     | f            | daily          | periodic        | {"file_name_format": "^.*(devices)_(\\d{8})_part_([0-9]+).*", "logical_file_name": "de
vices", "is_mandatory": true, "part_count": "*", "part_start_seq": 10} | config




WITH combined_file_arrival_status AS (
    SELECT feed_name, expectation_date, window_start_date, cadence_id, cutoff_date,
           is_adhoc_run, client_name, feed_frequency, extraction_type, output_folder 
    FROM file_arrival_status
    UNION ALL
    SELECT feed_name, expectation_date, window_start_date, cadence_id, cutoff_date,
           is_adhoc_run, client_name, feed_frequency, extraction_type, output_folder 
    FROM adhoc_file_arrival_status
),
feed_parts AS (
    SELECT 
        feed_name,
        CASE 
            WHEN bool_or(part_count = '*') THEN '*'
            ELSE '1'
        END AS part_count_flag
    FROM fas_cadence_config
    GROUP BY feed_name
)
SELECT DISTINCT fas.feed_name,
       COALESCE(fas.expectation_date, fas.window_start_date) AS expectation_date,
       cm.cadence_completion_status_sent_date,
       fas.cutoff_date,
       fas.is_adhoc_run,
       fas.client_name,
       fas.feed_frequency,
       fas.extraction_type,
       fas.window_start_date,
       fas.output_folder,
       cfc.feed_config->'extraction_settings'->>'e2e_custom_load_type' AS e2e_custom_load_type,
       fp.part_count_flag
FROM combined_file_arrival_status fas
JOIN cadence_master cm ON fas.cadence_id = cm.cadence_id
JOIN client_feed_config cfc ON fas.feed_name = cfc.feed_name
JOIN feed_parts fp ON fas.feed_name = fp.feed_name
WHERE cm.cadence_completion_flag IS NULL
  AND cfc.feed_config->>'is_active' = 'true';

