SELECT 
  CASE WHEN COUNT(*) = 1 THEN 1 ELSE 0 END AS success_flag 
FROM 
  file_arrival_status fas 
  JOIN file_master fm ON fm.cadence_id = fas.cadence_id 
  AND fm.feed_id = fas.feed_id 
WHERE 
  fas.file_name = 'TC011_claims_20240727_part_0001.csv' 
  AND fas.feed_name = 'centura_daily_autotestv2' 
  AND fas.client_name = 'regressionv1' 
  AND fas.logical_file_name = 'claims' 
  AND EXISTS (
    SELECT 
      1 
    FROM 
      unnest(fm.error_message) as unnested_error_message 
    WHERE 
      unnested_error_message like '%ErrorCode=MappingColumnNameNotFoundInSourceFile%'
  ) 
  AND EXISTS (
    SELECT 
      1 
    FROM 
      unnest(fm.error_source) as unnested_error_source 
    WHERE 
      unnested_error_source like '%adf%'
  ) 
  AND is_type_casting_success = 'f'
