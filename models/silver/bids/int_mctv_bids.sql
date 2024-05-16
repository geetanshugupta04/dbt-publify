with

    -- mctv_bids as (
    -- select
    -- *,
    -- case
    -- when
    -- lower(app_publisher_name) ilike "%jio%"
    -- or lower(app_app_name) ilike "%jio%"
    -- then "Jio"
    -- when
    -- lower(app_publisher_name) ilike "%viki%"
    -- or lower(app_app_name) ilike "%viki%"
    -- then "Viki"
    -- when
    -- lower(app_publisher_name) ilike "%yupptv%"
    -- or lower(app_app_name) ilike "%yupptv%"
    -- or lower(app_publisher_name) ilike "%wtodt-pim0g%"
    -- then "Samsung YuppTV"
    -- when
    -- lower(app_publisher_name) ilike "%tcl%"
    -- or lower(app_app_name) ilike "%tcl%"
    -- then "TCL Channel"
    -- else "Other"
    -- end as publisher
    -- from {{ ref("stg_mctv_bids") }}
    -- ),
    bids as (select * from {{ ref("stg_mctv_bids") }}),

    device_os_metadata as (select * from {{ ref("stg_device_os_metadata") }}),

    merged_with_device_os as (

        {{ merge_cleaned_device_os("bids", "device_os_metadata") }}

    ),

    device_data as (select * from {{ ref("int_device_master") }}),

    merged_with_device_data as (

        {{ merge_device_data("merged_with_device_os", "device_data") }}
    ),

    pincodes as (select * from {{ ref("stg_pincode_metadata") }}),

    merged_with_pincodes as (
        {{ merge_pincodes("merged_with_device_data", "pincodes") }}
    ),

    ssp_apps_tags as (select * from {{ ref("int_ssp_apps_tags") }}),

    ssp_publishers as (select * from {{ ref("int_ssp_publishers") }}),

    merged_with_ssp_apps_tags as (
        {{ merge_ssp_apps("merged_with_pincodes", "ssp_apps_tags") }}
    ),

    merged_with_ssp_publishers as (

        {{ merge_ssp_publishers("merged_with_ssp_apps_tags", "ssp_publishers") }}
    ),

    iab_categories as (select * from {{ ref("stg_metadata_content_categories") }}),

    merged_with_iab_categories as (

        {{ merge_iab_categories("merged_with_ssp_publishers", "iab_categories") }}

    ),

    final as (

        select

            ip,
            ifa,

            ssp,
            ad_type,

            device_os,
            cleaned_device_os,
            case
                when device_os = 'ipados'
                then 'ipados'
                when cleaned_device_os is null
                then 'NA'
                when device_os in ('samsung proprietary', 'tizen')
                then 'samsung tizen'
                when device_os in ('webos', 'lg proprietary')
                then 'lg webos'
                when device_os in ('chromeos')
                then 'google tv/chromecast'
                when device_os in ('tvos')
                then 'apple tvos'
                when device_os in ('vidaa')
                then 'hisense vidaa'
                when device_os in ('roku os')
                then 'roku os'
                else cleaned_device_os
            end as ctv_os,

            make,
            model,
            case
                when final_make is null
                then 'NA'
                when final_make in ('mi', 'xiaomi')
                then 'xiaomi'
                when
                    final_model ilike '%aft%'
                    or final_model = 'fire tv stick'
                    or final_model = 'amazon'
                then 'amazon fire tv stick'
                when
                    cleaned_device_os = 'Android'
                    and final_make in ('NA', 'unknown', 'generic android brand')
                then 'generic android brand'
                when
                    device_type = '7'
                    and (final_model = 'google tv stick' or final_make = 'google')
                then 'google chromecast'
                when final_model ilike '%tvos%'
                then 'apple tv'

                else final_make
            end as final_make,
            case when final_model is null then 'NA' else final_model end as final_model,
            cost,
            case
                when final_make = 'amazon fire tv stick' then '7' else device_type
            end as device_type,

            pincode,
            lat,
            lon,
            city,

            ssp_app_name,
            publify_app_name,

            case
                when final_make = 'jio'
                then 'Jio'
                when ssp_app_name ilike '%yupp%' or publify_app_name ilike '%yupp%'
                then 'Yupp TV'
                when ssp_app_name ilike '%viki%' or publify_app_name ilike '%viki%'
                then 'Viki'
                when ssp_app_name ilike '%dailymotion%'
                then 'DailyMotion'

                when ssp_app_name ilike '%bloomberg%'
                then 'Bloomberg'
                when ssp_app_name ilike '%apple%'
                then 'Apple TV'
                when ssp_app_name ilike '%fox%'
                then 'Fox News'
                else publify_app_name
            end as app_name,

            case
                when app_category is null then 'NA' else app_category
            end as app_category_tag,
            case
                when category_name is null then 'NA' else category_name
            end as iab_category,

            date,
            bid_count

        from merged_with_iab_categories
    )

select *
from final
