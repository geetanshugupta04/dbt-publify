-- inputs: campaign_id, c_line_item etc
-- sources
with

    -- bids as (select * from {{ ref("stg_bid_geo") }}),

    -- vast as (select * from {{ ref("stg_vast_geo") }}),

    track as (select * from {{ ref("stg_track_geo") }}),

    publishers as (select * from {{ ref("stg_publishers") }}),

    geo_map as (select * from {{ ref("stg_pincode_metadata") }}),

    -- central merge
    merged as (
        select
            b.date,
            b.ad_type,
            b.pincode,
            t.line_item_id,
            t.app_id,
            t.ssp,
            b.bid_requests,
            b.bid_response,
            v.bid_wins,
            t.impression,
            t.creative_view,
            t.click,
            t.complete
        from bids as b
        left join
            vast as v
            on b.date = v.date
            and b.ad_type = v.ad_type
            and b.pincode = v.pincode

        left join
            track as t
            on b.date = t.date
            and b.ad_type = t.ad_type
            and b.pincode = t.pincode

    ),


    -- pincode adjustments

    -- business adjustments

    -- geo merge
    geo_merge as (
        select
            m1.line_item_id,
            -- m1.line_item_type,
            -- m1.c_line_item_type,
            -- m1.campaign_id,
            m1.pincode,
            m1.date,
            m1.bid_requests,
            m1.bid_response,
            m1.bid_wins,
            m1.impression,
            m1.click,
            m1.complete,
            m1.creative_view,
            -- m1.campaign_name,
            -- m1.campaign_start_date,
            -- m1.campaign_end_date,
            -- m1.show_avg_cpm,
            geo_map.urban_or_rural,
            geo_map.city,
            geo_map.grand_city,
            geo_map.state
        from merged as m1
        left join geo_map on m1.pincode = geo_map.pincode
    )

select *
from geo_merge
