with

    audio_bids as (select * from {{ ref("int_bid_floor_audio") }}),

    cleaned_bids as (

        select

            ssp,
            ad_type,

            -- device_os,
            cleaned_device_os,

            -- make,
            -- model,
            final_make,
            final_model,

            age,
            gender,

            -- pincode,
            -- city,
            -- state,
            ssp_app_id,
            ssp_app_name,
            bundle,
            publisher_id,

            publify_app_name,
            ssp_publisher_id,
            ssp_publisher_name,
            publify_ssp_publisher_name,
            publisher_cleaned,
            publisher_final,

            -- category,
            -- genre,
            minduration,
            maxduration,

            floor_price as fp,
            sum(bids) as bids

        from audio_bids
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20

    ),

    weighted_means as (

        select
            cleaned_bids.*,
            {{
                calculate_weighted_mean(
                    "ad_type, ssp, publisher_id, publisher_final, publify_app_name",
                    "fp",
                    "bids",
                )
            }} as weighted_mean_pub_app,
            {{
                calculate_weighted_mean(
                    "ad_type, ssp, publisher_id, publisher_final, minduration, maxduration",
                    "fp",
                    "bids",
                )
            }}
            as weighted_mean_pub_duration
        from cleaned_bids

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
                    "ad_type, ssp, publisher_id, publisher_final, minduration, maxduration",
                    "weighted_mean_pub_duration",
                    "fp",
                    "bids",
                )
            }}
            as weighted_var_pub_duration

        from weighted_means as means

    ),

    weighted_stats as (

        select
            weighted.*,
            round(sqrt(weighted_var_pub_app), 6) as weighted_std_pub_app,
            round(sqrt(weighted_var_pub_duration), 6) as weighted_std_pub_duration

        from weighted_variance as weighted

    )

select *
from weighted_stats
order by 1, 2, 3, 4, 5, 6
