with

    bids as (select * from {{ ref("stg_bid_floor_audio") }}),

    device_os_metadata as (select * from {{ ref("stg_device_os_metadata") }}),

    merged_with_device_os as (

        {{ merge_cleaned_device_os("bids", "device_os_metadata") }}

    ),

    pincodes as (select * from {{ ref("stg_pincode_metadata") }}),

    merged_with_pincodes as ({{ merge_pincodes("merged_with_device_os", "pincodes") }}),

    ssp_apps as (select * from {{ ref("int_ssp_apps") }}),

    ssp_publishers as (select * from {{ ref("int_ssp_publishers") }}),

    merged_with_ssp_apps as ({{ merge_ssp_apps("merged_with_pincodes", "ssp_apps") }}),

    merged_with_ssp_apps_publishers as (

        {{ merge_ssp_publishers("merged_with_ssp_apps", "ssp_publishers") }}
    ),

    cleaned_bids as (
        select
            ssp,
            ad_type,
            dealcode,
            device_os,
            pincode,
            publisher_id,
            publify_ssp_publisher_name,
            publify_app_name,
            banner_height,
            banner_width,
            banner_position,
            banner_topframe,
            banner_fmt,
            fp,
            sum(bids) as bids

        from merged_with_ssp_apps_publishers
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14

    ),

    weighted_means as (

        select
            cleaned_bids.*,
            {{
                calculate_weighted_mean(
                    "ad_type, ssp, publisher_id, publify_ssp_publisher_name, publify_app_name",
                    "fp",
                    "bids",
                )
            }}
            as weighted_mean_pub_app,
            {{
                calculate_weighted_mean(
                    "ad_type, ssp, publisher_id, banner_height, banner_width",
                    "fp",
                    "bids",
                )
            }} as weighted_mean_pub_banner_dim
        from cleaned_bids

    ),

    weighted_variance as (

        select
            means.*,

            {{
                calculate_weighted_variance(
                    "ad_type, ssp, publisher_id, publify_ssp_publisher_name, publify_app_name",
                    "weighted_mean_pub_app",
                    "fp",
                    "bids",
                )
            }}
            as weighted_var_pub_app,
            {{
                calculate_weighted_variance(
                    "ad_type, ssp, publisher_id, banner_height, banner_width",
                    "weighted_mean_pub_banner_dim",
                    "fp",
                    "bids",
                )
            }} as weighted_var_pub_banner_dim

        from weighted_means as means

    ),

    weighted_stats as (

        select
            weighted.*,
            round(sqrt(weighted_var_pub_app), 6) as weighted_std_pub_app,
            round(sqrt(weighted_var_pub_banner_dim), 6) as weighted_std_pub_banner_dim

        from weighted_variance as weighted

    )

select *
from cleaned_bids
order by
    1,
    2,
    3,
    4,
    5,
    6
