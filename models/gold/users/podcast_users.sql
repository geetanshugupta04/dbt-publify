with

    int_podcast_bids as (select * from {{ ref("int_bid_floor_podcast") }}),

    podcast_users as (

        select
            ifa,
            ip,
            ad_type,
            ssp,
            cleaned_device_os,
            final_make,
            final_model,
            age,
            gender,
            city,
            iab_category_name,
            sum(bids) as bids

        from int_podcast_bids
        where ifa is not null
        group by all

    )

select *
from podcast_users
