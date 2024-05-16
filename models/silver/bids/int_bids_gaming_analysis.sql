with

    bids as (select * from {{ ref("stg_bids_for_gaming_analysis") }}),

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

    {#
    pincodes as (select * from {{ ref("stg_pincode_metadata") }}),

    merged_with_pincodes as ({{ merge_pincodes("merged_with_dealcodes", "pincodes") }}),

    #}
    ssp_apps_tags as (select * from {{ ref("int_ssp_apps_tags") }}),

    ssp_publishers as (select * from {{ ref("int_ssp_publishers") }}),

    merged_with_ssp_apps_tags as (
        {{ merge_ssp_apps("merged_with_dealcodes", "ssp_apps_tags") }}
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

            ssp,
            ad_type,

            ip,
            ifa,
            uid,

            cleaned_device_os,
            final_make,
            final_model,
            cost as device_cost,

            deal_0,
            age,
            gender,

            -- pincode,
            -- urban_or_rural,
            -- city,
            -- grand_city,
            -- state,
            ssp_app_id,
            ssp_app_name,
            bundle,
            publisher_id,

            case
                when publify_app_name is null then ssp_app_name else publify_app_name
            end as publify_app_name,
            case
                when publify_ssp_publisher_name is null
                then lower(ssp_publisher_name)
                else lower(publify_ssp_publisher_name)
            end as publisher_final,

            case
                when app_category is null then 'NA' else app_category
            end as app_category_tag,
            case
                when category_name is null then 'NA' else category_name
            end as iab_category,

            fp,
            date,
            bids

        from merged_with_iab_categories as bids
    )

select *
from final
