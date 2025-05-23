SELECT   fm.error_message  FROM file_arrival_status fas 
JOIN file_master fm 
ON fm.cadence_id = fas.cadence_id 
AND fm.feed_id = fas.feed_id 
WHERE fas.file_name in ('TC053_CLAIMS_09022025_part_2.txt' , 'TC053_CLAIMS_20250909_part_1.txt')
AND fas.feed_name = 'centura_monthly_autotest' 
AND fas.client_name = 'regressionv1' 
AND fas.logical_file_name = 'claims' 