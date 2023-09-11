with

publishers AS (
SELECT 
  id as app_id,
  name as publisher_group,
  ssp
FROM
  metadata_publisher_1_csv
  )

  select * from publishers