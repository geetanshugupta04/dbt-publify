with

    track as (select * from {{ ref("stg_track_oct_2") }}),

    track_cleaned as (
        select

            ad_type,
            platform_type,
            ssp,

            device_make,
            device_model,
            device_type,

            pincode,
            dealcode,

            app_id,
            category,
            domain,
            app_name,
            p_bundle,

            date,
            floor_currency,
            avg_floor_price,
            avg_bid_price,

            case when type = 'impression' then 1 else 0 end as impression,
            case when type = 'complete' then 1 else 0 end as complete,
            case when type = 'creativeview' then 1 else 0 end as creative_view,
            case
                when type in ('click', 'clicktracking', 'companionclicktracking')
                then 1
                else 0
            end as click

        from track
        where
            type in (
                'impression',
                'complete',
                'creativeview',
                'click',
                'clicktracking',
                'companionclicktracking'
            )

    ),

    track_grouped as (

        select

            ad_type,
            platform_type,
            ssp,

            device_make,
            device_model,
            device_type,

            pincode,
            dealcode,

            app_id,
            category,
            domain,
            app_name,
            p_bundle,

            date,
            floor_currency,
            avg_floor_price,
            avg_bid_price,

            sum(impression) as impression,
            sum(complete) as complete,
            sum(creative_view) as creative_view,
            sum(click) as click

        from track_cleaned
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17
    )

select *
from track_grouped
