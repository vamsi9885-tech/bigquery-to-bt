   select  case when count(*) = 5 then 1 else 0 end as success_flag 
   FROM 
  file_arrival_status fas 
  JOIN file_master fm ON fm.cadence_id = fas.cadence_id 
 WHERE 
  fas.feed_name = 'centura_monthly_autotest' 
  AND fas.client_name = 'regressionv1' 
  AND fas.logical_file_name in ('devices' , 'claims')
  and fm.file_name is not null 
  and fm.error_source = '{vm||adf}'
  and fm.file_name  = fas.file_name
  AND processed_row_count = source_row_count
  and fm.error_message = '{"NA || "}'
  and failed_row_count = 0
  and fm.file_name in ('TC046_devices_20250112_part_2.dat','TC046_devices_20250112_part_1.dat','TC046_CLAIMS_20250112_part_2.csv',
'TC046_CLAIMS_20250112_part_3.csv','TC046_CLAIMS_20250112_part_1.csv');