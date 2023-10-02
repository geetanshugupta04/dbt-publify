with

    pincode_data_for_ctr as (

        select *
        from {{ ref("int_overall_pincode_reports") }}
        where type = ['display']

    merge_ctr as (
        select
            pincode,
            "Bid Request",
            "Bid Response",
            "Bid Won",
            "Delivered Impressions",
            "Delivered companion Impressions",
            "Clicks",
            complete,
            {companion_zero_ctr} as ctr_impressions
        from merged2
    ),

    avg as (
        select
            pincode,
            coalesce(sum("Bid Request"), 0)::int as "Bid Request",
            coalesce(sum("Bid Response"), 0)::int as "Bid Response",
            coalesce(sum("Bid Won"), 0)::int as "Bid Won",
            coalesce(
                (
                    sum("Bid Request")
                    / ('{report_to}'::date - '{report_from}'::date + 1)
                ),
                0
            )::int as "Avg Bid Request per day",
            coalesce(
                (
                    sum("Bid Response")
                    / ('{report_to}'::date - '{report_from}'::date + 1)
                ),
                0
            )::int as "Avg Bid Response per day",
            coalesce(
                nullif(
                    concat(
                        round(
                            (sum("Bid Response") * 100 / nullif(sum("Bid Request"), 0)),
                            2
                        ),
                        '%'
                    ),
                    '%'
                ),
                '-'
            ) as "Response Percentage",
            coalesce(
                nullif(
                    concat(
                        round(
                            (sum("Bid Won") * 100 / nullif(sum("Bid Response"), 0)), 2
                        ),
                        '%'
                    ),
                    '%'
                ),
                '-'
            ) as "Win Percentage",
            coalesce(
                nullif(
                    concat(
                        round(
                            (
                                sum(complete)
                                * 100
                                / nullif(sum("Delivered Impressions"), 0)
                            ),
                            2
                        ),
                        '%'
                    ),
                    '%'
                ),
                '-'
            ) as "LTR",
            coalesce(
                nullif(
                    concat(
                        round(
                            (sum("Clicks") * 100 / nullif(sum(ctr_impressions), 0)), 2
                        ),
                        '%'
                    ),
                    '%'
                ),
                '-'
            ) as "CTR"
        from merge_ctr
        group by pincode
    ),

    final as (
        select
            city,
            pincode,
            "Bid Request",
            "Bid Response",
            "Bid Won",
            "Avg Bid Request per day",
            "Avg Bid Response per day",
            "Response Percentage",
            "Win Percentage",
            "LTR",
            "CTR"
        from merge_city
        order by "Bid Request" desc
    )

select *
from final
