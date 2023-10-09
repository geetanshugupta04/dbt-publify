with

    data_for_ctr_calculation as (

        select * from {{ ref("int_campaign_pincode_reports") }}
        
    ),

    -- ctr calculation
    merge_ctr as (
        select
            *,
            case
                when lower(c_line_item_type) in ('display', 'retargeted_banner')
                then impression
                else creative_view
            end as ctr_impressions
        from data_for_ctr_calculation
    ),

    semi_final as (

        select
            campaign_name,
            initcap(c_line_item_type) as c_line_item_type,
            city,
            show_avg_cpm,
            sum(impression)::int as delivered_impressions,
            cast((sum(creative_view)) as int) as delivered_companion_impressions,
            sum(click)::int as clicks,
            nullif(
                concat(
                    round((sum(complete) * 100 / nullif(sum(impression), 0)), 2), '%'
                ),
                '%'
            ) as vtr_ltr,
            nullif(
                concat(
                    round((sum(click) * 100 / nullif(sum(ctr_impressions), 0)), 2), '%'
                ),
                '%'
            ) as ctr
        from merge_ctr
        where city != '' and date <= (campaign_end_date + 2) and show_avg_cpm = true
        group by campaign_name, c_line_item_type, city, show_avg_cpm
        order by c_line_item_type, delivered_impressions desc
    ),

    final as (
        select
            campaign_name,
            c_line_item_type,
            city,
            delivered_impressions,
            (
                case
                    when lower(c_line_item_type) in ('display', 'retargeted_banner')
                    then 'NA'
                    else cast(delivered_companion_impressions as varchar(10))
                end
            ) as delivered_companion_impressions,
            clicks,
            case
                when lower(c_line_item_type) in ('display', 'retargeted_banner')
                then 'NA'
                else coalesce(vtr_ltr, '-')
            end as vtr_ltr,
            ctr
        from semi_final

    )

-- select *
-- from final
select
    campaign_name,
    sum(delivered_impressions),
    sum(clicks),
    sum(delivered_companion_impressions)

from final
group by 1
