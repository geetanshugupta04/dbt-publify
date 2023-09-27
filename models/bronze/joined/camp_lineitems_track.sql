with
    campaign as (select * from {{ ref("campaigns_specific") }}),

    lineitems as (select * from {{ ref("lineitems_specific") }}),

    track as (select * from {{ ref("track_geo_specific") }}),

    joined as (
        select

            c.campaign_name,
            c.campaign_start_date,
            c.campaign_end_date,
            c.show_avg_cpm,

            l.line_item_name,
            l.line_item_type,
            l.c_line_item_type,

            t.pincode,
            t.app_id,
            t.date,

            t.impression,
            t.creative_view,
            t.click,
            t.complete

        from track as t
        left join lineitems as l on t.line_item_id = l.line_item_id
        left join campaign as c on l.campaign_id = c.campaign_id

    )

select *
from joined
