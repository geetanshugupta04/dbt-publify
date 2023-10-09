with

    campaign as (
        select *
        from {{ ref("stg_campaigns") }}
        where campaign_id = 'ebpCArhq7MgQK25u6fzdg8'
    ),

    line_detail as (

        select *
        from {{ ref("stg_lineitems") }}
        where campaign_id in (select campaign_id from campaign)
    ),

    track_geo as (
        select *
        from {{ ref("stg_track_geo_campaigns") }}
        where line_item_id in (select line_item_id from lineitems)
    ),

    pub_detail as (select * from {{ ref("stg_publishers") }}),
    geo_map as (select * from {{ ref("stg_pincode_metadata") }}),

    /* offline pincode adjustments start*/
    track_pincode as (
        select
            line_item_id,
            pincode,
            app_id,
            date,
            ssp,
            sum(impression) as impression,
            sum(click) as click,
            sum(
                case when ssp = 'offline' then impression else complete end
            ) as complete,
            sum(
                case when ssp = 'offline' then impression else creative_view end
            ) as creative_view
        from track_geo
        group by line_item_id, pincode, app_id, date, ssp
    ),

    track_pincode_lineitems as (
        select l.campaign_id, l.c_line_item_type, t.*
        from track_pincode as t
        left join line_detail as l on t.line_item_id = l.line_item_id
    ),

    sum_by_pin as (
        select
            campaign_id,
            c_line_item_type,
            sum(impression) as sum_imp_pin,
            sum(click) as sum_click_pin,
            sum(complete) as sum_complete_pin,
            sum(creative_view) as sum_creative_view_pin
        from track_pincode_lineitems
        where pincode <> 'NA'
        group by campaign_id, c_line_item_type
    ),

    sum_na as (
        select
            campaign_id,
            c_line_item_type,

            sum(impression) as sum_imp_na,
            sum(click) as sum_click_na,
            sum(creative_view) as sum_complete_na,
            sum(complete) as sum_creative_view_na
        from track_pincode_lineitems
        where pincode = 'NA'
        group by campaign_id, c_line_item_type
    ),

    -- select * from sum_na
    sum_pincode_merged as (
        select
            imp.*,
            sum_pin.sum_imp_pin,
            sum_pin.sum_click_pin,
            (imp.impression / sum_pin.sum_imp_pin) as imp_percent,
            (imp.click / sum_pin.sum_click_pin) as click_percent,
            sum_pin.sum_complete_pin,
            (imp.complete / sum_pin.sum_complete_pin) as complete_percent,
            sum_pin.sum_creative_view_pin,
            (imp.creative_view / sum_pin.sum_creative_view_pin) as creative_view_percent
            case
                when sum_na.sum_imp_na is null then 0 else sum_na.sum_imp_na
            end as sum_imp_na,
            case
                when sum_na.sum_click_na is null then 0 else sum_na.sum_click_na
            end as sum_click_na,
            case
                when sum_na.sum_complete_na is null then 0 else sum_na.sum_complete_na
            end as sum_complete_na,
            case
                when sum_na.sum_creative_view_na is null
                then 0
                else sum_na.sum_creative_view_na
            end as sum_creative_view_na,

        from (select * from track_pincode_lineitems where pincode <> 'NA') as imp
        left join
            sum_by_pin as sum_pin
            on imp.c_line_item_type = sum_pin.c_line_item_type
            and imp.campaign_id = sum_pin.campaign_id
        left join
            sum_na
            on imp.c_line_item_type = sum_na.c_line_item_type
            and imp.campaign_id = sum_na.campaign_id

    ),

    -- select * from sum_pincode_merged
    track1_pincode_adjusted as (
        select
            campaign_id,
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
            creative_view,
            (complete + (sum_complete_na * complete_percent)) as delivered_complete,
            (
                creative_view + (sum_creative_view_na * creative_view_percent)
            ) as delivered_creative_view

        from sum_pincode_merged
    ),

    /* offline pincode adjustments end*/
  
    merged as (
        select t1.*, pub_detail.publisher_group, pub_detail.ssp
        from track1_pincode_adjusted t1
        left join pub_detail on t1.app_id = pub_detail.app_id
    ),

    track2 as (
        select
            line_item_id,
            pincode,
            app_id,
            date,
            publisher_group,
            ssp,
            sum(delivered_impressions) as impression,
            sum(delivered_clicks) as click,
            sum(delivered_complete) as complete,
            sum(delivered_creative_view) as creative_view

        from merged
        group by line_item_id, pincode, app_id, date, publisher_group, ssp
      
    ),

    /* Change creative view data for spotify, saavn  publisher*/
    track3 as (
        select
            line_item_id,
            pincode,
            app_id,
            date,
            publisher_group,
            ssp,
            sum(impression) as impression,
            sum(click) as click,
            sum(complete) as complete,
            sum(
                case
                    when lower(publisher_group) in ('spotify', 'saavn')
                    then impression
                    else creative_view
                end
            ) as creative_view
        from track2
        group by line_item_id, pincode, app_id, date, publisher_group, ssp
        
    ),

    /* Merge Line item metadata to line item- pub level data*/
    raw as (
        select
            t3.*,
            line_detail.campaign_id,
            line_detail.line_item_name,
            line_detail.line_item_type,
            line_detail.c_line_item_type
        from track3 t3
        left join line_detail on t3.line_item_id = line_detail.line_item_id
    ),

    /* Change data for Triton Audio  using global parameter  for 90% */
    track_triton as (
        select
            line_item_id,
            line_item_type,
            c_line_item_type,
            campaign_id,
            ssp,
            pincode,
            date,
            sum(impression) as impression,
            sum(click) as click,
            sum(complete) as complete,
            sum(
                case
                    when
                        lower(c_line_item_type)
                        in ('audio', 'audio_with_companion', 'video')
                    then impression
                    else creative_view
                end
            ) as creative_view
        from raw
        group by
            line_item_id,
            line_item_type,
            c_line_item_type,
            campaign_id,
            ssp,
            pincode,
            date
    ),

    /* Change data for line_item_type = display and   c_line_item_type  not display and agrregate data back to lineitem level*/
    track4 as (
        select
            line_item_id,
            line_item_type,
            c_line_item_type,
            campaign_id,
            pincode,
            to_date(date, 'YYYY-MM-DD') as date,
            sum(impression) as impression,
            sum(click) as click,
            sum(
                case
                    when
                        (lower(line_item_type)) = 'display'
                        and (lower(c_line_item_type))
                        not in ('display', 'retargeted_banner')
                    then impression
                    else complete
                end
            ) as complete,
            sum(
                case
                    when
                        (lower(line_item_type)) = 'display'
                        and (lower(c_line_item_type))
                        not in ('display', 'retargeted_banner')
                    then impression
                    else creative_view
                end
            ) as creative_view
        from track_triton
        group by
            line_item_id, line_item_type, c_line_item_type, campaign_id, pincode, date
        order by line_item_id, line_item_type, c_line_item_type, campaign_id, pincode
    ),

    merged1 as (
        select
            t4.*,
            camp_detail.campaign_name,
            camp_detail.campaign_start_date,
            camp_detail.campaign_end_date,
            camp_detail.show_avg_cpm
        from track4 t4
        left join campaign as camp_detail on t4.campaign_id = camp_detail.campaign_id
    ),

    merged2 as (
        select
            m1.*,
            geo_map.urban_or_rural,
            geo_map.city,
            geo_map.grand_city,
            geo_map.state
        from merged1 as m1
        left join geo_map on m1.pincode = geo_map.pincode
    )

select campaign_id, sum(impression), sum(click), sum(creative_view), sum(complete)
from merged2
group by 1
