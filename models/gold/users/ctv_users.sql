with

    int_ctv_bids as (select * from {{ ref("int_bid_floor_ctv") }}),

    ctv_users as (

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

            'NA' as age,
            'NA' as gender,

            city,

            case
                when genre is null then iab_category_name else genre
            end as genre,
            sum(bids) as bids

        from int_ctv_bids
        where ifa is not null or ip is not null or ipv6 is not null
        group by all
    )

select *
from ctv_users
