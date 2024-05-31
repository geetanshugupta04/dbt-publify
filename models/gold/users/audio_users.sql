with

    int_audio_bids as (select * from {{ ref("int_bid_floor_audio") }}),

    audio_users as (

        select

            ifa,
            ip,
            ipv6,

            lon,
            lat,

            year,
            month,
            day,
            hour,

            ad_type,
            ssp,

            device_type,
            cleaned_device_os,
            final_make,
            final_model,

            case when age is null then 'NA' else age end as age,
            case when gender is null then 'NA' else gender end as gender,

            city,

            iab_category_name as genre,
            sum(bids) as bids

        from int_audio_bids
        where ifa is not null or ip is not null or ipv6 is not null
        group by all
    )

select *
from audio_users
