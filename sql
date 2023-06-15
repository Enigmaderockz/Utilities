SELECT
  CASE
    WHEN max_date = '2023-04-19'
    THEN '2023-04-19'
    ELSE max_date
  END AS max_business_date
FROM
  (SELECT MAX(dt2_business) AS max_date FROM table) derived_table;
