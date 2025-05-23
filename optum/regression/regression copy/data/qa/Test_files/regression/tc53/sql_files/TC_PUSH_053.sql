SELECT   case when count(*) = 2 then 1 else 0 end as success_flag  FROM file_arrival_status fas 
JOIN file_master fm 
ON fm.cadence_id = fas.cadence_id 
AND fm.feed_id = fas.feed_id 
WHERE fas.file_name in ('TC053_CLAIMS_09022025_part_2.txt' , 'TC053_CLAIMS_20250909_part_1.txt')
AND fas.feed_name = 'centura_monthly_autotest' 
AND fas.client_name = 'regressionv1' 
AND fas.logical_file_name = 'claims' 
AND fm.arrived_flag = 't' 
AND fm.failed_row_count=0
AND processed_row_count=source_row_count 
AND is_type_casting_success = 't'