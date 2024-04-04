with

    bid_floors as (
        select * from {{ ref("int_publisher_ssp_division") }} where avg_floor_price < 10
    ),

    {% do log("fdgfsdv *******************************************", info=true) %}
{% do log(node, info=true) %}
    {% do log("fdgfsdv *******************************************", info=true) %}

    bid_floors_grouped as (

        select

            ssp,
            ad_type,
            publify_ssp_publisher_name,
            avg_floor_price as fp,
            sum(bid_counts) as bids

        from bid_floors
        group by 1, 2, 3, 4

    ),

    weighted_mean as (

        select
            ad_type,
            ssp,
            publify_ssp_publisher_name,
            fp,
            bids,
            {{
                calculate_weighted_mean(
                    "ad_type, ssp, publify_ssp_publisher_name", "fp", "bids"
                )
            }} as weighted_mean

        from bid_floors_grouped

    ),

    weighted_variance as (

        select
            weighted.*,

            sum(bids * power(fp - weighted_mean, 2)) over (
                partition by ad_type, ssp, publify_ssp_publisher_name
                range between unbounded preceding and unbounded following
            ) / sum(bids) over (
                partition by ad_type, ssp, publify_ssp_publisher_name
                range between unbounded preceding and unbounded following
            ) as weighted_variance

        from weighted_mean as weighted

    ),

    weighted_stats as (

        select weighted.*, round(sqrt(weighted_variance), 6) as weighted_std

        from weighted_variance as weighted

    )

-- select *
-- from weighted_stats
-- order by 1, 2, 3, 4
select *
from weighted_stats
