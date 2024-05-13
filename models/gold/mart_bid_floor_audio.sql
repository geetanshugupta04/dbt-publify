with

    audio_bids as (select * from {{ ref("int_bid_floor_audio") }}),

    cleaned_bids as (

        select

            ssp,
            case
                when deal_0 = 'PODCAST-3c68-4bde-89fd-2761ba68a499'
                then 'podcast'
                else ad_type
            end as ad_type,

            cleaned_device_os,

            final_make,
            final_model,

            deal_0,
            age,
            gender,

            pincode,
            city,

            ssp_app_id,
            ssp_app_name,
            bundle,
            publisher_id,

            case
                when publify_app_name is null then publisher_final else publify_app_name
            end as publify_app_final,

            case
                when publisher_final ilike 'grupo%'
                then 'grupo'
                when publisher_final ilike '%nzme%'
                then 'nzme'
                -- when
                -- publisher_final ilike '%audiohuis%'
                -- or ssp_publisher_name ilike '%audiohuis%'
                -- then 'audiohuis'
                when publisher_final ilike '%observador%'
                then 'observador'
                else publisher_final
            end as

            publisher_final,

            case
                when app_category_tag is null then 'NA' else app_category_tag
            end as app_category_tag,
            iab_category_name,
            genre,
            minduration,
            cast(maxduration as int) as maxduration,
            case when fp = 0 then 1 else 0 end as null_fps,

            round(cast(fp as float), 6) as fp,
            sum(bids) as bids

        from audio_bids
        where publisher_final not in ('tim media', 'light fm')
        group by all

    ),

    big_pubs_apps as (
        select
            bids.*,
            sum(bids) over (
                partition by ad_type, publisher_id, null_fps
            ) as pub_bids_sum_not_null,  -- sum_nulls get removed in the qualify clause
            sum(bids) over (
                partition by ad_type, publify_app_final, null_fps
            ) as pub_app_bids_sum_not_null,  -- sum_nulls get removed in the qualify clause
            sum(bids) over (partition by ad_type, publisher_id) as pub_bids_sum,
            sum(bids) over (
                partition by ad_type, publify_app_final
            ) as pub_app_bids_sum,
            sum(bids) over (partition by ad_type) as total_bids_sum

        from cleaned_bids as bids
        qualify (pub_app_bids_sum > 100) and null_fps = 0

    ),

    weighted_means as (

        select
            bids.*,
            {{
                calculate_weighted_mean(
                    "ad_type, ssp, publisher_id, publify_app_final",
                    "fp",
                    "bids",
                )
            }} as weighted_mean_pub_app,
            {{
                calculate_weighted_mean(
                    "ad_type, ssp, publisher_id, deal_0",
                    "fp",
                    "bids",
                )
            }} as weighted_mean_pub_deal,
            {{
                calculate_weighted_mean(
                    "ad_type, ssp, app_category_tag",
                    "fp",
                    "bids",
                )
            }} as weighted_mean_app_category,
            {{
                calculate_weighted_mean(
                    "ad_type, ssp, age, gender",
                    "fp",
                    "bids",
                )
            }} as weighted_mean_age_gender
        from big_pubs_apps as bids

    ),

    weighted_stats as (

        select
            means.*,

            sqrt(
                {{
                    calculate_weighted_variance(
                        "ad_type, ssp, publisher_id, publify_app_final",
                        "weighted_mean_pub_app",
                        "fp",
                        "bids",
                    )
                }}
            ) as weighted_std_pub_app,
            sqrt(
                {{
                    calculate_weighted_variance(
                        "ad_type, ssp, publisher_id, deal_0",
                        "weighted_mean_pub_deal",
                        "fp",
                        "bids",
                    )
                }}
            ) as weighted_std_pub_deal,
            sqrt(
                {{
                    calculate_weighted_variance(
                        "ad_type, ssp, app_category_tag",
                        "weighted_mean_app_category",
                        "fp",
                        "bids",
                    )
                }}
            ) as weighted_std_app_category,
            sqrt(
                {{
                    calculate_weighted_variance(
                        "ad_type, ssp, age, gender",
                        "weighted_mean_age_gender",
                        "fp",
                        "bids",
                    )
                }}
            ) as weighted_std_age_gender

        from weighted_means as means

    )

select *
from weighted_stats
