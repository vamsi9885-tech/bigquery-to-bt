
   SELECT  case when count(*) = 1 then 1 else 0 end as success_flag 
 FROM 
  file_arrival_status fas 
  JOIN file_master fm ON fm.cadence_id = fas.cadence_id 
 WHERE 
  fas.feed_name = 'centura_daily_autotest' 
  and fm.file_name = 'TC038_claims_20240820_part_0001.csv'
  and fm.file_name = fas.file_name
  AND fas.client_name = 'regressionv1' 
  AND fas.logical_file_name = 'claims'  
  AND processed_row_count = source_row_count
	and fm.error_message = '{"NA || "}'
	and fm.error_source = '{vm||adf}'
  and failed_row_count = 2;