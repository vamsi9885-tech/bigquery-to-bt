SELECT  case when count(distinct fm.file_name ) = 2 then 1 else 0 end as success_flag
FROM 
 file_arrival_status fas 
 JOIN file_master fm ON fm.cadence_id = fas.cadence_id 
WHERE 
 fas.feed_name = 'centura_daily_autotest' 
 and fm.file_name like 'TC047%'
 AND fas.client_name = 'regressionv1' 
 AND fas.logical_file_name = 'claims' 
 AND fm.error_message = '{"NA || "}' 
 AND fm.error_source = '{vm||adf}' 
 and source_row_count = processed_row_count
 and is_type_casting_success = 't' ;