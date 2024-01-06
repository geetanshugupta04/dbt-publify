with

    bids as (select * from {{ ref("stg_bids_dec11") }}),

    device_os_metadata as (select * from {{ ref("stg_device_os_metadata") }}),

    merged_with_device_os as (

        select bids.*, device_os_metadata.cleaned_device_os
        from bids
        left join device_os_metadata on bids.device_os = device_os_metadata.device_os

    ),

    merged_with_device_os_cleaned as (

        select
            merged_with_device_os.*,
            case
                when cleaned_device_os is null and lower(device_os) = 'ios'
                then 'iOS'
                when cleaned_device_os is null and lower(device_os) = 'android'
                then 'Android'
                when
                    cleaned_device_os is null
                    and lower(device_os) in ('linux', 'linux - ubuntu')
                then 'Linux'
                when cleaned_device_os is null and lower(device_os) = 'ipados'
                then 'iPadOS'
                when
                    cleaned_device_os is null
                    and lower(device_os) in ('macos', 'mac os', 'os x')
                then 'macOS'
                when cleaned_device_os is null and lower(device_os) = 'chrome os'
                then 'Chrome OS'
                when
                    cleaned_device_os is null
                    and lower(device_os) in ('windows 10', 'windows 7', 'windows')
                then 'Windows'
                when
                    cleaned_device_os is null
                    and lower(device_os) = 'samsung proprietary'
                then 'Samsung'
                when cleaned_device_os is null and lower(device_os) = 'tvos'
                then 'Apple TV OS'
                when cleaned_device_os is null and device_os in ('Other', 'Not Found')
                then 'NA'
                else cleaned_device_os
            end as cleaned_device_os_2

        from merged_with_device_os

    )

    -- merged_with_device_os_agg as (
    --     select

    --         -- ssp,
    --         -- ad_type,
    --         cleaned_device_os_2,
    --         -- cleaned_device_os,
    --         -- device_os,
    --         sum(bid_count) as bids,
    --         avg(avg_floor_price) as avg_price_floor

    --     from merged_with_device_os_cleaned
    --     group by 1

    -- )

select *
from merged_with_device_os_cleaned
