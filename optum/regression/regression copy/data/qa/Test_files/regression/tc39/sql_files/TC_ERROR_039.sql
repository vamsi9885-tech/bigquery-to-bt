 SELECT  fm.error_message
 FROM 
  file_arrival_status fas 
  JOIN file_master fm ON fm.cadence_id = fas.cadence_id 
 WHERE 
  fas.feed_name = 'centura_daily_autotest' 
  and fm.file_name = 'TC039_claims_20240821_part_0001.csv'
  and fm.file_name = fas.file_name
  AND fas.client_name = 'regressionv1' 
  AND fas.logical_file_name = 'claims'  ;
