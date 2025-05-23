 SELECT 
 CASE WHEN COUNT(*) = 3 THEN 1 ELSE 0 END AS success_flag 
FROM 
  file_arrival_status fas 
  JOIN file_master fm ON fm.cadence_id = fas.cadence_id 
WHERE 
  fas.feed_name = 'centura_daily_autotest' and 
fas.window_start_date = '2024-06-16' and fm.arrival_date = current_date
And fas.file_name = fm.file_name
and fm.file_name in ( 'TC006_claims_20240616_part_001.csv' ,'TC006_devices_20240616_part_001.dat'  ,'TC006_payers_20240616_part_0001.csv')
and fm.is_type_casting_success = 't'
and processed_row_count = source_row_count 
AND fm.error_message = '{"NA || "}'
AND fm.error_source = '{vm||adf}'
and fm.failed_row_count = 0 ;