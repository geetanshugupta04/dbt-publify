TRACK1 as (
SELECT 
    line_item_id,
    coalesce(pincode, 'NA') as pincode,
    app_id,
    date,
    SUM(impression) as impression,
    SUM(creative_view) as creative_view, 
    SUM(click) as click,
    SUM(complete) as complete
FROM
	track_geo_hourly
WHERE 
    line_item_id in (
      SELECT line_item_id from Line_detail)
GROUP BY 
    line_item_id,
    pincode,
   app_id,
    date
),


track_pincode as (
  select
    line_item_id,
    pincode,
    app_id,
    date,
    sum(impression) as impression,
    sum(click) as click,
    sum(complete) as complete,
    sum(creative_view) as creative_view
  from
    track1
	
  group by
    line_item_id,
    pincode,
    app_id,
    date
),


track_pincode_lineitems as (
	select 
	l.campaign_id,
	l.c_line_item_type,
    t.*
  
  from track_pincode as t
  left join Line_detail as l
  on t.line_item_id = l.line_item_id

),

sum_by_pin as (
  select
	campaign_id,
    c_line_item_type,
    sum(impression) as sum_imp_pin,
    sum(click) as sum_click_pin
  from
    track_pincode_lineitems
  where
    pincode <> 'NA'
  group by
	campaign_id,
    c_line_item_type
),

sum_na as (
  select
	campaign_id,
        c_line_item_type,
 
    sum(impression) as sum_imp_na,
    sum(click) as sum_click_na
  from
    track_pincode_lineitems
  where
    pincode = 'NA'
  group by
	campaign_id,
    c_line_item_type
),


sum_pincode_merged as (
  select
    imp.*,
    sum_pin.sum_imp_pin,
    sum_na.sum_imp_na,
    sum_pin.sum_click_pin,
    sum_na.sum_click_na,
    (
      imp.impression / sum_pin.sum_imp_pin
    ) as imp_percent,
    (
      imp.click / sum_pin.sum_click_pin
    ) as click_percent
  from
    (
      select
        *
      from
        track_pincode_lineitems
      where
        pincode <> 'NA'
    ) as imp
    left join sum_by_pin as sum_pin 
	on 
	imp.c_line_item_type = sum_pin.c_line_item_type
	and 
	imp.campaign_id = sum_pin.campaign_id
    left join sum_na 
	on 
	imp.c_line_item_type = sum_na.c_line_item_type
	and
	imp.campaign_id = sum_na.campaign_id

    ),
	
    
track1_pincode_adjusted as (
  select
    c_line_item_type,
	line_item_id,
    app_id,
    date,
    pincode,
    impression,
    sum_imp_pin,
    sum_imp_na,
    imp_percent,
    (impression + (sum_imp_na * imp_percent)) as delivered_impressions,
    (impression + (sum_imp_na * imp_percent)) - impression as diff_imp,
    click,
    sum_click_pin,
    sum_click_na,
    click_percent,
    (click + (sum_click_na * click_percent)) as delivered_clicks,
    (click + (sum_click_na * click_percent)) - click as diff_click,
	complete,
    creative_view
    
  from
    sum_pincode_merged
),


-- select 
-- c_line_item_type,
-- sum(delivered_impressions) as delivered_imp,
-- sum(diff_imp) as diff_imp,
-- sum(delivered_clicks) as del_clicks,
-- sum(diff_click) as diff_clicks
-- from track1_pincode_adjusted
-- group by
-- c_line_item_type
