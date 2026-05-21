SELECT 
  user_id,
  email,
  age,
  current_timestamp() as processed_time
FROM 
  sandbox.fed_bronze.users