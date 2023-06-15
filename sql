SELECT
  CASE
    WHEN EXISTS (SELECT 1 FROM table WHERE dt2_business = '2023-04-19')
    THEN '2023-04-19'
    ELSE (SELECT MAX(dt2_business) FROM table)
  END AS max_business_date;
