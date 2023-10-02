with

data_for_ctr_calculation as (

    select * from {{ ref('int_campaign_pincode_reports') }}
).

    -- ctr calculation
ctr as (
        select
            line_item_id,
            line_item_type,
            c_line_item_type,
            campaign_id,
            impression,
            click,
            date,
            complete,
            creative_view,
            campaign_name,
            campaign_end_date,
            city,
            grand_city,
            show_avg_cpm,
            case
                when lower(c_line_item_type) in ('display', 'retargeted_banner')
                then impression
                else creative_view
            end as ctr_impressions
        from data_for_ctr_calculation
    ),

    final as (
        select
            campaign_name as "Campaign Name",
            initcap(c_line_item_type) as c_line_item_type,
            case
                when grand_city = '' then city else grand_city
            end as "Grand City/City",
            sum(impression) as "Delivered Impressions",
            cast((sum(creative_view)) as int) as "Delivered companion Impressions",
            sum(click) as "Clicks",
            nullif(
                concat(
                    round((sum(complete) * 100 / nullif(sum(impression), 0)), 2), '%'
                ),
                '%'
            ) as "VTR/LTR",
            nullif(
                concat(
                    round((sum(click) * 100 / nullif(sum(ctr_impressions), 0)), 2), '%'
                ),
                '%'
            ) as "CTR"
        from merge_ctr
        where date <= (campaign_end_date + 2) and show_avg_cpm = true
        group by "Campaign Name", c_line_item_type, "Grand City/City"

    ),

    final2 as (

        select
            "Campaign Name",
            c_line_item_type,
            "Grand City/City",
            "Delivered Impressions"::int as "Delivered Impressions",
            case
                when lower(c_line_item_type) in ('display', 'retargeted_banner')
                then 'NA'
                else "Delivered companion Impressions"::varchar
            end as "Delivered Companion Impressions",
            "Clicks"::int as "Clicks",
            case
                when lower(c_line_item_type) in ('display', 'retargeted_banner')
                then 'NA'
                else coalesce("VTR/LTR", '-')
            end as "VTR/LTR",
            coalesce("CTR", '-') as "CTR"
        from final
        where "Grand City/City" != ''
        order by c_line_item_type, "Delivered Impressions" desc

    )

select *
from final2
