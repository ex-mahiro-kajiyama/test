CREATE OR REPLACE TABLE prefect_test.new_pengin AS
SELECT 
  species,
  year
FROM
  `prefect_test.pengin`
LIMIT
  10
;