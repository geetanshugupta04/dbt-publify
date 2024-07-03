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

            year,
            month,
            day,
            -- hour,
            cleaned_device_os,
            device_type,

            final_make,
            final_model,

            deal_0,
            age,
            gender,
            ip,
            -- ipv6,
            ifa,

            -- lon,
            -- lat,
            pincode,
            city,

            ssp_app_id,
            ssp_app_name,
            bundle,
            publisher_id,

            case
                when publify_app is null then publify_publisher else publify_app
            end as app_final,

            case
                when publify_publisher ilike 'grupo%'
                then 'grupo'
                when publify_publisher ilike '%nzme%'
                then 'nzme'
                -- when
                -- publisher_final ilike '%audiohuis%'
                -- or ssp_publisher_name ilike '%audiohuis%'
                -- then 'audiohuis'
                when publify_publisher ilike '%observador%'
                then 'observador'
                else publify_publisher
            end as

            publisher_final,

            case
                when app_category_tag is null then 'NA' else app_category_tag
            end as app_category_tag,
            iab_category_name,
            itunes_category,

            case when genre is null then 'NA' else genre end as genre,
            -- case when series is null then 'NA' else series end as series,
            minduration,
            cast(maxduration as int) as maxduration,
            case when fp is null or fp > 999 then 1 else 0 end as null_fps,

            round(cast(fp as float), 6) as fp,
            sum(bids) as bids

        from audio_bids
        where ad_type = 'audio' and publify_publisher not in ('tim media', 'light fm')
        group by all

    ),

    big_apps as (
        select
            bids.*,
            sum(bids) over (partition by ssp) as ssp_bids_sum,  -- sum_nulls get removed in the qualify clause
            sum(bids) over (partition by ssp, null_fps) as ssp_bids_sum_not_null,  -- sum_nulls get removed in the qualify clause
            sum(bids) over (partition by app_final) as app_bids_sum,
            sum(bids) over (partition by app_final, null_fps) as app_bids_sum_not_null,  -- sum_nulls get removed in the qualify clause
            sum(bids) over (partition by app_category_tag) as tag_bids_sum,
            sum(bids) over (
                partition by app_category_tag, null_fps
            ) as tag_bids_sum_not_null,
            sum(bids) over (partition by ssp, app_category_tag) as ssp_tag_bids_sum,
            sum(bids) over (
                partition by ssp, app_category_tag, null_fps
            ) as ssp_tag_bids_sum_not_null,
            sum(bids) over () as total_bids_sum

        from cleaned_bids as bids
        where ad_type = 'audio'
        qualify (app_bids_sum > 5000)

    )

{#

    weighted_means as (

        select
            bids.*,
            {{
                calculate_weighted_mean(
                    "ad_type, ssp, publisher_id, app_final",
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

    )



    weighted_stats as (

        select
            means.*,

            sqrt(
                {{
                    calculate_weighted_variance(
                        "ad_type, ssp, publisher_id, app_final",
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

    #}
select *
from big_apps
