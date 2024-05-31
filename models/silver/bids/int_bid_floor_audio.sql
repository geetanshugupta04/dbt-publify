with

    bids as (select * from {{ ref("stg_bid_floor_audio") }}),

    device_os_metadata as (select * from {{ ref("stg_device_os_metadata") }}),

    merged_with_device_os as (

        {{ merge_cleaned_device_os("bids", "device_os_metadata") }}

    ),

    device_data as (select * from {{ ref("int_device_master") }}),

    merged_with_device_data as (

        {{ merge_device_data("merged_with_device_os", "device_data") }}
    ),

    dealcodes as (select * from {{ ref("stg_dealcodes") }}),

    merged_with_dealcodes as (
        {{ merge_dealcodes("merged_with_device_data", "dealcodes") }}
    ),

    pincodes as (select * from {{ ref("stg_pincode_metadata") }}),

    merged_with_pincodes as ({{ merge_pincodes("merged_with_dealcodes", "pincodes") }}),

    ssp_apps_tags as (select * from {{ ref("int_ssp_apps_tags") }}),

    ssp_publishers as (select * from {{ ref("int_ssp_publishers") }}),

    merged_with_ssp_apps_tags as (
        {{ merge_ssp_apps("merged_with_pincodes", "ssp_apps_tags") }}
    ),

    merged_with_ssp_publishers as (

        {{ merge_ssp_publishers("merged_with_ssp_apps_tags", "ssp_publishers") }}
    ),

    iab_categories as (select * from {{ ref("int_iab_content_categories") }}),

    merged_with_iab_categories as (

        {{ merge_iab_categories("merged_with_ssp_publishers", "iab_categories") }}

    ),

    final as (

        select

            ssp,
            ad_type,
            deal_0,

            year,
            month,
            day,
            hour,

            device_os,
            cleaned_device_os,
            device_type,

            model,
            make,
            final_make,
            final_model,

            ip,
            ipv6,
            ifa,
            age,
            gender,

            lon,
            lat,
            pincode,
            urban_or_rural,
            city,
            grand_city,
            state,

            ssp_app_id,
            ssp_app_name,
            bundle,
            domain,
            publisher_id,

            publify_app_name as publify_app,
            case
                when publify_ssp_publisher_name is null
                then lower(ssp_publisher_name)
                else lower(publify_ssp_publisher_name)
            end as publify_publisher,

            app_category as app_category_tag,
            category,
            category_name as iab_category_name,
            itunes_category,
            genre,
            -- case when series is null then 'NA' else series end as series,
            minduration,
            maxduration,
            startdelay,
            -- maxextended,
            maxseq,
            stitched,

            fp,
            bids

        from merged_with_iab_categories as bids
    )

select *
from final
