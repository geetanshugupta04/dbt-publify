
-- inputs: campaign_id, c_line_item etc


-- sources
with

    campaign as (select * from {{ ref("stg_campaigns") }} where campaign_id = ''),

    lineitems as (

        select *
        from {{ ref("stg_lineitems") }}
        where campaign_id in (select campaign_id from campaign)
    ),

    track_geo as (

        select *
        from
            {{ ref("stg_track_geo") }}
            wnere line_item_id in (select line_item_id from lineitems)
    ),

    publishers as (select * from {{ ref("stg_publishers") }}),

    geo_map as (
        select *
        from {{ ref("stg_pincode_metadata") }}
        where pincode in (select pincode from track_geo)
    ),

    -- track lineitem merge
    track_pincode_lineitems as (
        select l.campaign_id, l.c_line_item_type, t.*

        from track1 as t
        left join line_detail as l on t.line_item_id = l.line_item_id

    ),

    -- pincode adjustments
    track1_pincode_adjusted as (
        select
            c_line_item_type,
            line_item_id,
            app_id,
            date,
            pincode,
            -- impression,
            -- sum_imp_pin,
            -- sum_imp_na,
            -- imp_percent,
            (impression + (sum_imp_na * imp_percent)) as impression,
            -- (impression + (sum_imp_na * imp_percent)) - impression as diff_imp,
            -- click,
            -- sum_click_pin,
            -- sum_click_na,
            -- click_percent,
            (click + (sum_click_na * click_percent)) as click,
            -- (click + (sum_click_na * click_percent)) - click as diff_click,
            complete,
            creative_view

        from sum_pincode_merged
    ),

    -- joins with campaign and publisher tables
    merged as (
        select
            t1.line_item_id,
            t1.pincode,
            t1.app_id,
            t1.date,
            t1.impression,
            t1.creative_view,
            t1.click,
            t1.complete,
            pub_detail."Publisher Group",
            pub_detail.ssp
        from track1_pincode_adjusted t1
        left join pub_detail on t1.app_id = pub_detail.app_id
    ),

    -- business adjustments
    track2 as (
        select
            line_item_id,
            pincode,
            app_id,
            date,
            "Publisher Group",
            ssp,
            sum(impression) as impression,
            sum(click) as click,
            sum(
                case when ssp = 'offline' then impression else complete end
            ) as complete,
            sum(
                case when ssp = 'offline' then impression else creative_view end
            ) as creative_view
        from merged
        group by line_item_id, pincode, app_id, date, "Publisher Group", ssp
    ),

    -- geo merge
    geo_merge as (
        select
            m1.line_item_id,
            m1.line_item_type,
            m1.c_line_item_type,
            m1.campaign_id,
            m1.pincode,
            m1.date,
            m1.impression,
            m1.click,
            m1.complete,
            m1.creative_view,
            m1.campaign_name,
            m1.campaign_start_date,
            m1.campaign_end_date,
            m1.show_avg_cpm,
            geo_map.urban_or_rural,
            geo_map.city,
            geo_map.grand_city,
            geo_map.state
        from merged1 m1
        left join geo_map on m1.pincode = geo_map.pincode
    )


select * from geo_merge