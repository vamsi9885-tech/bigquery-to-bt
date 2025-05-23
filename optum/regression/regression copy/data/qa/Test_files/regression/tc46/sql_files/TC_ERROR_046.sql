  select  distinct error_message
   FROM 
  file_arrival_status fas 
  JOIN file_master fm ON fm.cadence_id = fas.cadence_id 
 WHERE 
	  fas.feed_name = 'centura_monthly_autotest' 
  AND fas.client_name = 'regressionv1' 
  AND fas.logical_file_name in ('devices' , 'claims')
  AND fm.error_message is not null  limit 1;

