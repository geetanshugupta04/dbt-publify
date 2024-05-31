with

    int_video_bids as (select * from {{ ref("int_bid_floor_video") }}),

    video_users as (
-- databricks s3 dapi18a6a4938cce73bc23a4a4451f457afa
        select
            ifa,
            ip,
            ad_type,
            ssp,
            cleaned_device_os,
            final_make,
            final_model,
            'NA' as age,
            'NA' as gender,
            city,
            category_name as iab_category_name,
            sum(bids) as bids
        from int_video_bids
        where ifa is not null
        group by all
    )

select *
from video_users
