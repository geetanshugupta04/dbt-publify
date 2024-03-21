with

    display_bids as (select * from {{ ref("stg_bid_floor_display") }} limit 10000),

    ssp_apps as (select * from {{ ref("int_ssp_apps") }}),

    ssp_publishers as (select * from {{ ref("int_ssp_publishers") }}),

    merged_with_ssp_apps as ({{ merge_ssp_apps("display_bids", "ssp_apps") }}),

    merged_with_ssp_apps_publishers as (

        {{ merge_ssp_publishers("merged_with_ssp_apps", "ssp_publishers") }}
    ),

    cleaned_bids as (
        select
            ssp,
            ad_type,
            publisher_id,
            publify_ssp_publisher_name,
            publify_app_name,
            fp,
            sum(bids) as bids

        from merged_with_ssp_apps_publishers
        group by 1, 2, 3, 4, 5, 6

    ),

    weighted_mean as (

        select
            ssp,
            ad_type,
            publisher_id,
            publify_ssp_publisher_name,
            publify_app_name,
            fp,
            bids,
            {{
                calculate_weighted_mean(
                    "ad_type, ssp, publisher_id, publify_ssp_publisher_name, publify_app_name", fp, bids
                )
            }} as weighted_mean

        from cleaned_bids

    ),

    weighted_variance as (

        select
            weighted.*,

            sum(bids * power(fp - weighted_mean, 2)) over (
                partition by ad_type, ssp, publisher_id, publify_ssp_publisher_name, publify_app_name
                range between unbounded preceding and unbounded following
            ) / sum(bids) over (
                partition by ad_type, ssp, publisher_id, publify_ssp_publisher_name, publify_app_name
                range between unbounded preceding and unbounded following
            ) as weighted_variance

        from weighted_mean as weighted

    ),

weighted_stats as (

        select weighted.*, round(sqrt(weighted_variance), 6) as weighted_std

        from weighted_variance as weighted

    )


select *
from
    weighted_stats
    order by 1,2,3,4,5,6

    /*


17094 - 11464
20616 - 11393


*/
    -- bids 2664823
    -- apps 
    -- pubs 2664823
    
