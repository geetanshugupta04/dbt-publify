with

    track as (select * from {{ ref("int_track_grouped") }}),

    pincodes as (
        select * from {{ ref("stg_pincode_metadata") }} where is_verified = 'true'
    ),

    device_os_metadata as (select * from {{ ref("stg_device_os_metadata") }}),

    ssp_apps as (select * from {{ ref("int_ssp_apps") }}),

    track_abridged as (

        select
            ssp,
            ad_type,
            platform_type,
            -- device_make,
            -- device_model,
            device_os,
            pincode,
            -- app_name,
            date,
            hour,
            sum(impression) as impression,
            sum(complete) as complete,
            sum(creative_view) as creative_view,
            sum(click) as click

        from track
        group by 1, 2, 3, 4, 5, 6, 7
    ),

    track_abridged_cleaned as (
        select
            ssp,
            ad_type,
            platform_type,
            -- device_make,
            -- device_model,
            device_os,
            pincode,
            -- app_name,
            date,
            hour,
            case
                when impression < creative_view
                then creative_view
                when impression < complete
                then complete
                else impression
            end as impression,
            complete,
            creative_view,
            click

        from track_abridged

    ),

    merged_with_pincodes as (
        select track.*, pincodes.city, pincodes.state

        from track_abridged_cleaned as track
        left join pincodes on track.pincode = pincodes.pincode
    ),

    merged_with_pincodes_deviceos as (

        select merged_with_pincodes.*, device_os_metadata.cleaned_device_os

        from merged_with_pincodes
        left join
            device_os_metadata
            on merged_with_pincodes.device_os = device_os_metadata.device_os

    ),

    final as (

        select
            ssp,
            ad_type,
            platform_type,
            -- device_make,
            -- device_model,
            -- cleaned_device_os,
            case
                when cleaned_device_os is null and lower(device_os) = 'ios'
                then 'iOS'
                when cleaned_device_os is null and lower(device_os) = 'android'
                then 'Android'
                -- when
                --     cleaned_device_os is null
                --     and lower(device_os) in ('linux', 'linux - ubuntu')
                -- then 'Linux'
                -- when cleaned_device_os is null and lower(device_os) = 'ipados'
                -- then 'iPadOS'
                -- when
                --     cleaned_device_os is null
                --     and lower(device_os) in ('macos', 'mac os', 'os x')
                -- then 'macOS'
                -- when cleaned_device_os is null and lower(device_os) = 'chrome os'
                -- then 'Chrome OS'
                -- when
                --     cleaned_device_os is null
                --     and lower(device_os) in ('windows 10', 'windows 7', 'windows')
                -- then 'Windows'
                -- when
                --     cleaned_device_os is null
                --     and lower(device_os) = 'samsung proprietary'
                -- then 'Samsung'
                -- when cleaned_device_os is null and lower(device_os) = 'tvos'
                -- then 'Apple TV OS'
                -- when cleaned_device_os is null and device_os in ('Other', 'Not Found')
                -- then 'NA'
                else 'Other'
            end as cleaned_device_os,

            -- app_name,
            city,
            state,
            date,
            hour,
            sum(impression) as impression,
            sum(complete) as complete,
            sum(creative_view) as creative_view,
            sum(click) as click

        from merged_with_pincodes_deviceos
        where city is not null
        group by 1, 2, 3, 4, 5, 6, 7, 8

    )

select *
from final
where ssp is not null and ssp != 'offline'

