with
    smart_tv_homes as (

        select ip from {{ ref("mart_bid_floor_ctv") }} where is_home_ip = 1
    ),

    display as (
        select

            ip,
            ifa,
            ad_type,
            cleaned_device_os,
            city,
            app_category_tag,
            iab_category_name,
            sum(bids) as bids
        from {{ ref("mart_bid_floor_display") }}
        inner join smart_tv_homes using (ip)
        group by all
        order by all
    )

select *
from display
