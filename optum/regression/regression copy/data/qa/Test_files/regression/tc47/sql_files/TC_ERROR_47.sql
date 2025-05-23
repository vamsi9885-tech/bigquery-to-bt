SELECT  error_message 
FROM 
 file_arrival_status fas 
 JOIN file_master fm ON fm.cadence_id = fas.cadence_id 
WHERE 
 fas.feed_name = 'centura_daily_autotest' 
 and fm.file_name like 'TC047%'
 AND fas.client_name = 'regressionv1' 
 AND fas.logical_file_name = 'claims' ;