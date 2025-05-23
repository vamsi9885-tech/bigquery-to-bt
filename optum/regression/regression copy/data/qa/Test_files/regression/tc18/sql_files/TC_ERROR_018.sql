SELECT fm.error_message
FROM 
  file_arrival_status fas 
  JOIN file_master fm ON fm.cadence_id = fas.cadence_id 
  AND fm.feed_id = fas.feed_id 
WHERE 
  fas.file_name = 'TC018_devices_20240805_part_0001.dat' 
  AND fas.feed_name = 'centura_daily_autotest' 
  AND fas.client_name = 'regressionv1' 
  AND fas.logical_file_name = 'devices';
 
