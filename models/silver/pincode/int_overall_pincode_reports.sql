-- inputs: campaign_id, c_line_item etc
-- sources
with

    bid_geo as (
        select *
        from {{ ref("stg_bid_geo") }}
        where
            date((start_datetime at time zone 'Asia/Kolkata'))
            between ('{report_from}') and ('{report_to}')
            and ad_type in ({ad_type})
            and country in ({country})
    ),

    vast_geo as (
        * from {{ ref("stg_vast_geo") }}
        where
            date((start_datetime at time zone 'Asia/Kolkata'))
            between ('{report_from}') and ('{report_to}')
            and ad_type in ({ad_type})
            and country in ({country})

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

    -- central merge
    bid_vast_track_merge as (
        select
            b.pincode,
            b."Bid Request",
            b."Bid Response",
            v."Bid Won"
            m.pincode,
            m."Bid Request",
            m."Bid Response",
            m."Bid Won",
            t."Delivered Impressions",
            t."Delivered companion Impressions",
            t."Clicks",
            t.complete
        from merged m
        left join track t on m.pincode = t.pincode
        left join vast v on b.pincode = v.pincode

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

select *
from geo_merge
