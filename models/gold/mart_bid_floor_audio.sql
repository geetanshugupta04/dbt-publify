with

    audio_bids as (select * from {{ ref("int_bid_floor_audio") }}),

    cleaned_bids as (

        select

            ssp,
            ad_type,

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

            publify_app_name,

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
                when app_category is null then 'NA' else app_category
            end as app_category_tag,
            category_name,
            genre,
            minduration,
            round(cast(maxduration as int) / 10) as maxduration,

            round(cast(fp as float), 6) as fp,
            sum(bids) as bids

        from audio_bids
        where publisher_final not in ('tim media', 'light fm')
        group by
            1,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
            9,
            10,
            11,
            12,
            13,
            14,
            15,
            16,
            17,
            18,
            19,
            20,
            21,
            22
    -- having fp < 5
    ),

    big_pubs as (

        select ad_type, publisher_final, sum(bids) as bids
        from cleaned_bids
        group by 1, 2
        having bids > 100

    ),

    big_fps as (

        select fp, sum(bids) as bids from cleaned_bids group by 1 having bids > 10

    ),

    big_pub_big_fp_bids as (

        select *
        from cleaned_bids
        where
            publisher_final in (select publisher_final from big_pubs)
            and fp in (select fp from big_fps)
    ),

    weighted_means as (

        select
            bids.*,
            {{
                calculate_weighted_mean(
                    "ad_type, ssp, publisher_id, publisher_final, publify_app_name",
                    "fp",
                    "bids",
                )
            }} as weighted_mean_pub_app,
            {{
                calculate_weighted_mean(
                    "ad_type, ssp, publisher_id, publisher_final, deal_0",
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
        from big_pub_big_fp_bids as bids

    ),

    weighted_variance as (

        select
            means.*,

            {{
                calculate_weighted_variance(
                    "ad_type, ssp, publisher_id, publisher_final, publify_app_name",
                    "weighted_mean_pub_app",
                    "fp",
                    "bids",
                )
            }} as weighted_var_pub_app,
            {{
                calculate_weighted_variance(
                    "ad_type, ssp, publisher_id, publisher_final, deal_0",
                    "weighted_mean_pub_deal",
                    "fp",
                    "bids",
                )
            }} as weighted_var_pub_deal,
            {{
                calculate_weighted_variance(
                    "ad_type, ssp, app_category_tag",
                    "weighted_mean_app_category",
                    "fp",
                    "bids",
                )
            }} as weighted_var_app_category,
            {{
                calculate_weighted_variance(
                    "ad_type, ssp, age, gender",
                    "weighted_mean_age_gender",
                    "fp",
                    "bids",
                )
            }} as weighted_var_age_gender

        from weighted_means as means

    ),

    weighted_stats as (

        select
            weighted.*,
            round(sqrt(weighted_var_pub_app), 6) as weighted_std_pub_app,
            round(sqrt(weighted_var_pub_deal), 6) as weighted_std_pub_deal,
            round(sqrt(weighted_var_app_category), 6) as weighted_std_app_category,
            round(sqrt(weighted_var_age_gender), 6) as weighted_std_age_gender

        from weighted_variance as weighted

    )

select *
from weighted_stats
