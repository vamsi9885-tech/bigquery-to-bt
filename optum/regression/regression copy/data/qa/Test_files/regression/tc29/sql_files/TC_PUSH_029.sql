SELECT 
  CASE WHEN COUNT(*) = 1 THEN 1 ELSE 0 END AS success_flag 
FROM 
  file_arrival_status fas 
  JOIN file_master fm ON fm.cadence_id = fas.cadence_id 
  AND fm.feed_id = fas.feed_id 
WHERE 
  fas.file_name = 'TC029_devices_20240930_part_0001.csv' 
  AND fas.feed_name = 'centura_daily_autotest' 
  AND fas.client_name = 'regressionv1' 
  AND fas.logical_file_name = 'devices' 
  AND fm.arrived_flag = 't'
