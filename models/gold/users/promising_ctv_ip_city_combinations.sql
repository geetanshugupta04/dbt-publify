with

    ctv_users as (select * from {{ ref("ctv_users") }}),

    audio_users as (select * from {{ ref("audio_users") }}),

    display_users as (select * from {{ ref("display_users") }}),

    -- ip and city collectively constitutes as a unique combination
    -- we have taken those ip city combinations that have exactly 1 smart TV device
    ctv_ips as (

        select coalesce(ip, ipv6) as ip, lat, lon, city, count(distinct ifa) as ifas
        from ctv_users
        where
            (ip is not null or ipv6 is not null)
            and device_type in (3)
            and final_make not in ('amazon', 'google')
            and city is not null

        group by all
        having ifas = 1

    ),

    -- after shortlisting the city-ip combinations with exactly one smart TV device,
    -- we find the number of audio and display devices linked to each of those city-ip
    -- combinations and restrict them to a max of 5 devices (if either of audio or
    -- display have greater than 5 devices, we drop those)
    promising_city_ip_combinations as (

        select

            ctv_ips.ip,
            ctv_ips.city,
            ctv_ips.lat,
            ctv_ips.lon,
            count(distinct audio_users.ifa) as audio_ifas,
            count(distinct display_users.ifa) as display_ifas

        from ctv_ips

        left join
            audio_users
            on audio_users.ip = ctv_ips.ip
            and audio_users.city = ctv_ips.city
        left join
            display_users
            on display_users.ip = ctv_ips.ip
            and display_users.city = ctv_ips.city
        where audio_users.ifa is not null or display_users.ifa is not null
        group by all
        having not (audio_ifas > 5 or display_ifas > 5)

    )

select *
from promising_city_ip_combinations
