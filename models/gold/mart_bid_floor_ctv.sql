with

    ctv_bids as (select * from {{ ref("int_bid_floor_ctv") }}),

    cleaned_bids as (

        select

            ssp,
            ad_type,
            year,
            month,
            day,
            hour,

            coalesce(ip, ipv6) as ip,
            ifa,

            cleaned_device_os,
            device_type,
            final_make,
            final_model,

            pincode,
            city,
            lon,
            lat,

            case
                when ssp_app_name ilike '%yupp%tv%' or bundle ilike '%yupp%tv%'
                then 'YuppTV'
                when ssp_app_name ilike '%viki%' or bundle ilike '%viki%'
                then 'Viki'
                when bundle ilike '%jioplay%'
                then 'Jio Play'
                when bundle ilike '%jio%screensaver%'
                then 'Jio Screensaver'
                when bundle ilike '%jio%ondemand%'
                then 'Jio On Demand'
                when bundle = 'com.tencent.qqlivei18n'
                then 'WeTV Asia'
                when ssp_app_name ilike '%fox%' or bundle ilike '%fox%'
                then 'Fox News'
                else publify_app
            end as app_final,

            case
                when ssp_app_name ilike '%yupp%tv%' or bundle ilike '%yupp%tv%'
                then 'YuppTV'
                when ssp_app_name ilike '%viki%' or bundle ilike '%viki%'
                then 'Viki'
                when publify_app = 'TCL CHANNEL'
                then 'TCL Corporation'
                when bundle = 'com.tencent.qqlivei18n'
                then 'WeTV Asia'
                else publify_publisher
            end as publisher_final,

            app_category_tag,
            category,
            iab_category_name,
            contentrating,
            livestream,
            genre,
            network,
            channel,
            prodq,

            h,
            w,
            linearity,
            instl,
            placement,

            minduration,
            maxduration,
            maxextended,
            skip,
            skipmin,
            skipafter,
            startdelay,

            fp,

            sum(bids) as bids

        from ctv_bids
        where
            ifa is not null
            and ifa not in ('0000-0000', '00000000-0000-0000-0000-000000000000')
        group by all
        having not (app_final = 'YuppTV' and app_final != publisher_final)

    ),

    home_ips as (
        select ip, count(distinct ifa) as ifa_counts

        from cleaned_bids
        where device_type = 3
        group by all
        having ifa_counts < 5
    ),

    final as (

        select
            cleaned_bids.*,
            case when ip in (select ip from home_ips) then 1 else 0 end as is_home_ip

        from cleaned_bids
    )

select *
from final
