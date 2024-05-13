with

    video_bids as (select * from {{ ref("int_bid_floor_video") }}),

    cleaned_bids as (

        select

            ssp,
            ad_type,

            cleaned_device_os,

            final_make,
            final_model,

            pincode,
            city,
            urban_or_rural,

            ssp_app_id,
            ssp_app_name,
            bundle,
            publisher_id,

            case
                when publify_app_name is null then ssp_app_name else publify_app_name
            end as publify_app_name,
            publisher_final,

            app_category as app_category_tag,
            category_name as iab_category_name,

            h,
            ln(h) as lnh,
            w,
            ln(h * w) as lnhw,

            placement,
            /*
            1. in stream - played before, during or after the streaming video content (pre, mid, post roll)
            2. in banner - exists within a web banner, leverages the banner space to deliver a video experience
            3. in article - loads and plays dynamically between paragraphs of editorial content
            4. in feed - 
            5. interstitial/slider/floating - covers the entire or a portion of the screen area, but is always on screen and cannot be scrolled out of view. Full screen interstial vs floating/slider.


            */
            skip,  -- lower is better, 0 = cannot skip, 1 = possible to skip
            case
                when skip = 0
                then 0
                when skip = 1 and skipmin = 99999
                then 0
                else skipmin
            end as skipmin,
            case when skip = 0 then 0 else skipafter end as skipafter,

            case when startdelay > 0 then 0 else startdelay end as startdelay,
            -- 0 = pre roll, -1 = generic mid roll, -2 = generic post roll, >0 = start
            -- delay in seconds (mid roll)
            -- be careful when including more data, currently max(startdealy) was just 2
            -- so currently higher is better
            case when linearity = 2 then 0 else linearity end as linearity,
            -- originally lower is better, 1 = Instream (user is forced to watch the
            -- ad), 2 = Overlay (possible to skip)
            -- changed to higher is better, 1 = Instream, 0 = Overlay
            minduration,
            maxduration,
            ln(maxduration) as lnmaxd,

            fp,
            ln(fp * 100) as lnfp,
            case when fp = 99999 then 1 else 0 end as null_fps,
            sum(bids) as bids

        from video_bids
        where
            h not in (-1, 0, 1, 99999)
            and maxduration not in (99999, 2147483647)
            and cleaned_device_os
            in ('iOS', 'Linux', 'macOS', 'Chrome OS', 'Android', 'Windows')
            and placement in ('1', '5')
            and fp < 100000
            and publify_app_name is not null

        group by all

    ),

    big_pubs_apps as (
        select
            bids.*,
            sum(bids) over (
                partition by publisher_id, null_fps
            ) as pub_bids_sum_not_null,  -- sum_nulls get removed in the qualify clause
            sum(bids) over (
                partition by ssp_app_name, null_fps
            ) as ssp_app_bids_sum_not_null,  -- sum_nulls get removed in the qualify clause
            sum(bids) over (
                partition by publify_app_name, null_fps
            ) as pub_app_bids_sum_not_null,
            sum(bids) over (partition by publisher_id) as pub_bids_sum,
            sum(bids) over (partition by ssp_app_name) as ssp_app_bids_sum,
            sum(bids) over (partition by publify_app_name) as pub_app_bids_sum,
            sum(bids) over () as total_bids_sum

        from cleaned_bids as bids
        qualify pub_app_bids_sum > 100000 and null_fps = 0

    ),

    weighted_means as (

        select
            bids.*,
            {{
                calculate_weighted_mean(
                    "ad_type, ssp, publisher_id, ssp_app_name",
                    "lnfp",
                    "bids",
                )
            }} as wmean_lnfp_ssp_app,
            {{
                calculate_weighted_mean(
                    "ad_type, ssp, publify_app_name",
                    "lnfp",
                    "bids",
                )
            }} as wmean_lnfp_pub_app,
            {{
                calculate_weighted_mean(
                    "ad_type, ssp, publisher_id, ssp_app_name",
                    "lnmaxd",
                    "bids",
                )
            }} as wmean_maxd,
            {{
                calculate_weighted_mean(
                    "ad_type, ssp, publisher_id, ssp_app_name",
                    "lnh",
                    "bids",
                )
            }} as wmean_h

        from big_pubs_apps as bids

    ),

    weighted_std as (

        select
            means.*,

            sqrt(
                {{
                    calculate_weighted_variance(
                        "ad_type, ssp, publisher_id, ssp_app_name",
                        "wmean_lnfp_ssp_app",
                        "lnfp",
                        "bids",
                    )
                }}
            ) as wstd_lnfp_ssp_app,
            sqrt(
                {{
                    calculate_weighted_variance(
                        "ad_type, ssp, publify_app_name",
                        "wmean_lnfp_pub_app",
                        "lnfp",
                        "bids",
                    )
                }}
            ) as wstd_lnfp_pub_app,
            sqrt(
                {{
                    calculate_weighted_variance(
                        "ad_type, ssp, publisher_id, ssp_app_name",
                        "wmean_maxd",
                        "lnmaxd",
                        "bids",
                    )
                }}
            ) as wstd_maxd,
            sqrt(
                {{
                    calculate_weighted_variance(
                        "ad_type, ssp, publisher_id, ssp_app_name",
                        "wmean_h",
                        "lnh",
                        "bids",
                    )
                }}
            ) as wstd_h

        from weighted_means as means

    ),

    weighted_stats as (

        select
            weighted.*,
            round(
                pub_bids_sum_not_null / pub_bids_sum, 2
            ) as pub_percent_bids_not_null_fp,
            round(
                ssp_app_bids_sum_not_null / ssp_app_bids_sum, 2
            ) as ssp_app_percent_bids_not_null_fp,
            round(
                pub_app_bids_sum_not_null / pub_app_bids_sum, 2
            ) as pub_app_percent_bids_not_null_fp,
            (lnfp - wmean_lnfp_ssp_app) / wstd_lnfp_ssp_app as zlnfp_ssp_app,
            (lnfp - wmean_lnfp_pub_app) / wstd_lnfp_pub_app as zlnfp_pub_app,
            (lnmaxd - wmean_maxd) / wstd_maxd as zlnmaxd,
            (lnh - wmean_h) / wstd_h as zlnh

        from weighted_std as weighted

    )

select *
from weighted_stats
