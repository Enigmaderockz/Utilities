SELECT 
  CASE
    WHEN COUNT(*) > 0 THEN '2023-04-19' -- If the specified date is present, return it
    ELSE MAX(DT2_BUSINESS) -- If the specified date is not present, fetch the maximum business_date from the table
  END AS max_business_date
FROM
  table
WHERE
  DT2_BUSINESS = '2023-04-19' OR -- Check if the specified date is present
  NOT EXISTS (SELECT 1 FROM table WHERE DT2_BUSINESS = '2023-04-19') -- If the specified date is not present, fetch the maximum business_date from the table
