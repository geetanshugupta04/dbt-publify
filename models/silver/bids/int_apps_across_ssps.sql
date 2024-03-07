with

    int_ssp_app_merge as (select * from {{ ref("int_ssp_app_merge") }}),

    apps_grouped as (

        select ad_type, publify_app_name, ssp, sum(bid_counts) as bids, count(*)
        from int_ssp_app_merge
        group by 1, 2, 3

    ),

    apps_grouped_counts as (

        select ad_type, publify_app_name, count(*) as counts
        from apps_grouped
        where publify_app_name is not null and publify_app_name not in ('-')
        group by 1, 2
        having counts > 1
    ),

    apps_across_ssps as (

        select
            apps.*,
            case
                when
                    publify_app_name
                    in (select publify_app_name from apps_grouped_counts)
                then 1
                else 0
            end as is_across_ssps

        from int_ssp_app_merge as apps

    )

select *
from apps_across_ssps
order by ad_type, publify_app_name, ssp
