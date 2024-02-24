with

    camp_detail as (
        select * from {{ ref("stg_campaigns") }}
    -- where campaign_id = '9BeKeQKcfQCD2gmCBvvFvM'
    ),

    line_detail as (

        select *
        from {{ ref("stg_lineitems") }}
        where campaign_id in (select campaign_id from camp_detail)
    ),

    track_pub_hourly as (
        select *
        from {{ ref("stg_track_pub_hourly") }}
        where line_item_id in (select line_item_id from line_detail)
    ),

    pub_detail as (select * from {{ ref("stg_publishers") }}),

    deals as (
        select * from {{ ref("stg_dealcodes") }}
    -- where is_active is true
    ),

    /* Take out impression data*/
    track as (
        select
            line_item_id,
            dealcode,
            -- app_id,
            -- date,
            sum(impression) as impression,
            sum(creative_view) as creative_view,
            sum(click) as click,
            sum(complete) as complete
        from track_pub_hourly
        where line_item_id in (select line_item_id from line_detail)
        group by 1, 2

    ),

    track_deal_merge as (
        select
            track.*,
            deals.line_item_type,
            deals.deal_type,
            deals.deal_id,
            deals.age,
            deals.gender,
            camp_detail.campaign_name

        from track
        left join deals on track.dealcode = deals.deal_id
        left join line_detail on track.line_item_id = line_detail.line_item_id
        left join camp_detail on line_detail.campaign_id = camp_detail.campaign_id

    ),

    track_age_gender as (
        select
            campaign_name,
            line_item_id,
            dealcode,
            deal_id,
            -- app_id,
            -- date,
            age,
            gender,
            sum(impression) as impression,
            sum(creative_view) as creative_view,
            sum(click) as click,
            sum(complete) as complete

        from track_deal_merge
        group by 1, 2, 3, 4, 5, 6

    )

select *
from track_age_gender
