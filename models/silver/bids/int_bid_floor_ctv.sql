{% set numerical_columns_with_nulls = [
    "h",
    "w",
    "fp",
    "skip",
    "skipafter",
    "startdelay",
    "skipmin",
    "linearity",
    "minduration",
    "maxduration",
] %}
{% set categorical_columns_with_nulls = [
    "final_make",
    "final_model",
    "cleaned_device_os",
    "app_category",
    "category_name",
    "placement",
] %}

with

    bids as (select * from {{ ref("stg_bid_floor_ctv") }}),

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

    iab_categories as (select * from {{ ref("int_iab_content_categories") }}),

    merged_with_iab_categories as (

        {{ merge_iab_categories("merged_with_ssp_publishers", "iab_categories") }}

    ),

    final as (

        select

            ssp,
            ad_type,

            year,
            month,
            day,
            hour,

            ip,
            ipv6,
            ifa,

            device_os,
            cleaned_device_os,
            device_type,

            model,
            make,
            final_make,
            final_model,

            pincode,
            urban_or_rural,
            city,
            state,
            lon,
            lat,

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
            {#
            -- {% for column in categorical_columns_with_nulls %}
            --     case
            --         when {{ column }} is null then 'NA' else {{ column }}
            --     end as {{ column }},
            -- {% endfor %}

            -- {% for column in numerical_columns_with_nulls %}
            --     case
            --         when {{ column }} is null then 99999 else {{ column }}
            --     end as {{ column }},
            -- {% endfor %}
            #}
            h,
            w,
            skip,
            skipafter,
            skipmin,
            startdelay,

            linearity,
            minduration,
            maxduration,
            maxextended,
            instl,
            placement,

            category,
            app_category as app_category_tag,
            category_name as iab_category_name,
            contentrating,
            livestream,
            genre,
            network,
            channel,
            prodq,

            floor_price as fp,
            bid_count as bids

        from merged_with_iab_categories as bids
    )

select *
from final
