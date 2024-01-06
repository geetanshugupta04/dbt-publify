with

    track as (select * from {{ ref("stg_track_dec10") }}),

    track_cleaned as (
        select
            ssp,
            ad_type,
            platform_type,

            device_make,
            device_model,
            device_os,

            pincode,

            floor_price,
            bid_price,

            id,
            appid,
            bundle,
            bundle_name,
            app_publisher_id,
            domain,
            app_name,
            p_bundle,
            app_id,

            category,
            content_category,

            date,
            date_trunc('minute', saved_on) as datetime,
            date_trunc('hour', saved_on) as hour,

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

            ssp,
            ad_type,
            platform_type,

            device_make,
            device_model,
            device_os,

            pincode,

            floor_price,
            bid_price,

            id,
            appid,
            bundle,
            bundle_name,
            app_publisher_id,
            domain,
            app_name,
            p_bundle,
            app_id,

            category,
            content_category,

            date,
            hour,
            datetime,

            sum(impression) as impression,
            sum(complete) as complete,
            sum(creative_view) as creative_view,
            sum(click) as click

        from track_cleaned
        group by
            1,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
            9,
            10,
            11,
            12,
            13,
            14,
            15,
            16,
            17,
            18,
            19,
            20,
            21,
            22,
            23

    )

select *
from track_grouped
