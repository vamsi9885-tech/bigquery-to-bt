 SELECT 
 fm.error_message
FROM 
  file_arrival_status fas 
  JOIN file_master fm ON fm.cadence_id = fas.cadence_id 
WHERE 
  fas.feed_name = 'centura_daily_autotest' and 
fas.window_start_date = '2024-06-16' and fm.arrival_date = current_date
and fm.file_name in ( 'TC006_claims_20240616_part_001.csv' ,'TC006_devices_20240616_part_001.dat'  ,'TC006_payers_20240616_part_0001.csv');