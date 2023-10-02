
with

device_data_for_ctr as (

    select * from {{ ref('int_overall_device_reports') }}
),

ctr as (
  select
	date,
    coalesce(company_make, '-') as make,
    -- 	device_make,
    coalesce(master_model, '-') as model,
    -- 	device_model,
    coalesce(cleaned_device_os, '-') as device_os,
    coalesce(device_type, '-') as device_type,
    coalesce(cast(release_month as varchar), '-') as release_month,
    coalesce(release_year, '-') as release_year,
    coalesce(cast(cost as varchar), '-') as cost,
    coalesce(bid_requests, 0) :: int as "Bid Request",
    coalesce(bid_response, 0) :: int as "Bid Response",
    coalesce(bid_wins, 0) :: int as "Bid Won",
    coalesce(impressions, 0) :: int as "Delivered Impressions",
    coalesce(creative_view, 0) :: int as "Delivered Companion Impressions",
    coalesce(clicks, 0) :: int as "Clicks",
    coalesce(complete, 0) :: int as "Complete",
    coalesce(
      (
        bid_requests /('{end_date}' :: date - '{start_date}' :: date + 1)
      ),
      0
    ) :: int as "Avg Bid Request per day",
    coalesce(
      (
        bid_response /('{end_date}' :: date - '{start_date}' :: date + 1)
      ),
      0
    ) :: int as "Avg Bid Response per day",
    coalesce(
      nullif(
        concat(round(bid_response * 100 / bid_requests, 2), '%'),
        '%'
      ),
      '-'
    ) as "Response Percentage",
    coalesce(
      nullif(
        concat(
          round(bid_wins * 100 / nullif(bid_response, 0), 2),
          '%'
        ),
        '%'
      ),
      '-'
    ) as "Win Percentage",
    coalesce(
      nullif(
        concat(
          round(complete * 100 / nullif(impressions, 0), 2),
          '%'
        ),
        '%'
      ),
      '-'
    ) as "LTR",
    coalesce(
      nullif(
        concat(round(clicks * 100 / nullif(impressions, 0), 2), '%'),
        '%'
      ),
      '-'
    ) as "CTR"
  from
    device_data_for_ctr
	
)

select * from ctr

