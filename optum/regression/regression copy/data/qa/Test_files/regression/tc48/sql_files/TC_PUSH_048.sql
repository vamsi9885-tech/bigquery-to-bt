 select    count(distinct fas.cadence_id)   as success_flag
  FROM 
  file_arrival_status fas 
  JOIN file_master fm ON fm.cadence_id = fas.cadence_id 
 WHERE 
  fas.feed_name = 'centura_monthly_autotest' 
  AND fas.client_name = 'regressionv1' 
  AND fas.logical_file_name = 'claims' 
  and fm.file_name is not null 
  and fm.file_name = fas.file_name
  and fas.file_name in ('TC048_CLAIMS_20250211_part_3.txt','TC048_CLAIMS_20250309_part_2.txt')
  and fm.error_message = '{"NA || "}'
  AND fm.error_source = '{vm||adf}' 
  and processed_row_count = source_row_count
  and is_type_casting_success = 't';