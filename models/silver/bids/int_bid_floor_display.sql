with

    bids as (select * from {{ ref("stg_bid_floor_display") }}),

    device_os_metadata as (select * from {{ ref("stg_device_os_metadata") }}),

    merged_with_device_os as (

        {{ merge_cleaned_device_os("bids", "device_os_metadata") }}

    ),

    pincodes as (select * from {{ ref("stg_pincode_metadata") }}),

    merged_with_pincodes as ({{ merge_pincodes("merged_with_device_os", "pincodes") }}),

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

            ssp,
            ad_type,

            cleaned_device_os,
            device_type,
            device_lang,

            pincode,
            urban_or_rural,
            city,
            state,

            ssp_app_id,
            ssp_app_name,
            bundle,
            publisher_id,

            case
                when publify_app_name is null then ssp_app_name else publify_app_name
            end as publify_app_final,
            case
                when publify_ssp_publisher_name is null
                then lower(ssp_publisher_name)
                else lower(publify_ssp_publisher_name)
            end as publisher_final,

            app_category,
            category_name,

            pos,
            h,
            w,
            fmt,
            instl,

            fp,
            bids

        from merged_with_iab_categories

    )

select *
from final
