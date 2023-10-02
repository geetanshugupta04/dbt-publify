with

    data_for_ctr_calculation as (

        select * from {{ ref("int_campaign_device_reports") }}
    ),

    merge_ctr as (
        select *, {display_ctr_impression} as ctr_impressions

        from merged2
        where date <= (campaign_end_date + 2) and show_avg_cpm = true

    ),

    final as (
        select
            initcap(c_line_item_type) as "Type",
            company as "Company",
            model_name as "Model Name",
            sum(impression)::int as "Delivered Impressions",
            case
                when lower(c_line_item_type) in ('display', 'retargeted_banner')
                then 'NA'
                else cast(sum(creative_view) as varchar)
            end as "Delivered Companion Impressions",
            sum(click)::int as "Clicks",
            case
                when lower(c_line_item_type) in ('display', 'retargeted_banner')
                then 'NA'
                else
                    coalesce(
                        nullif(
                            concat(
                                round(
                                    (sum(complete) * 100 / nullif(sum(impression), 0)),
                                    2
                                ),
                                '%'
                            ),
                            '%'
                        ),
                        '-'
                    )
            end as "VTR/LTR",
            coalesce(
                nullif(
                    concat(
                        round((sum(click) * 100 / nullif(sum(ctr_impressions), 0)), 2),
                        '%'
                    ),
                    '%'
                ),
                '-'
            ) as "CTR"
        from merge_ctr
        group by c_line_item_type, "Company", "Model Name"
        order by c_line_item_type, "Delivered Impressions" desc

    )

select *
from final
