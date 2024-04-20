with

    bids as (select * from {{ ref("stg_bid_floor_video") }}),

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

    iab_categories as (select * from {{ ref("stg_ad_categories") }}),

    merged_with_iab_categories as (

        {{ merge_iab_categories("merged_with_ssp_publishers", "iab_categories") }}

    ),

    final as (

        select

            ssp,
            ad_type,

            device_os,
            cleaned_device_os,

            model,
            make,
            final_make,
            final_model,

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

            publify_app_name,
            case
                when publify_ssp_publisher_name is null
                then lower(ssp_publisher_name)
                else lower(publify_ssp_publisher_name)
            end as publisher_final,

            app_category,
            category,
            category_name,

            h,
            w,
            placement,
            skipafter,
            skip,
            startdelay,
            minduration,
            maxduration,
            linearity,
            skipmin,

            companion_banner_h0,
            companion_banner_h1,
            companion_banner_h2,
            companion_banner_h3,
            companion_banner_w0,
            companion_banner_w1,
            companion_banner_w2,
            companion_banner_w3,
            companion_banner_h4,
            companion_banner_h5,
            companion_banner_h6,
            companion_banner_h7,
            companion_banner_w4,
            companion_banner_w5,
            companion_banner_w6,
            companion_banner_w7,

            case when floor_price is null then 0 else floor_price end as fp,
            date,
            bid_count as bids

        from merged_with_iab_categories as bids
    )

select *
from final
