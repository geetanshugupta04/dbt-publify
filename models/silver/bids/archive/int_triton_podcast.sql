{% set site_app_variables = [
    "cat",
    "publisher_name",
    "content_language",
    "content_context",
    "content_prodq",
    "content_len",
    "content_genre",
    "content_id",
    "content_url",
    "content_title",
    "content_episode",
    "content_season",
    "content_series",
] %}

with

    triton_podcast_bids as (
        select * from {{ ref("stg_bids_triton_podcast_jan5_20_22") }}
    ),

    triton_podcast_bids_abridged as (

        select

            platform_type,

            user_gender as gender,
            (2024 - cast(user_yob as int)) as age,
            device_ifa_type,
            device_ifa,
            device_language,
            device_model,
            case
                when lower(device_device_os) in ('ios', 'android')
                then lower(device_device_os)
                else 'other'
            end as device_os,
            device_device_type as device_type,
            device_pincode,
            device_country,
            device_geo_type,
            device_lon,
            device_lat,
            device_ip,
            device_ua,

            {%- set site = "site_" %} {%- set app = "app_" %}
            {%- for variable in site_app_variables %}
                case
                    when platform_type = 'site'
                    then {{ site }}{{ variable }}
                    else {{ app }}{{ variable }}
                end as {{ variable }},
            {% endfor -%}

            -- maxseq,  -- the max no. of ads that can be played in an ad pod
            delivery,  -- delivery options like streaming, progressive, download
            -- maxbitrate,  -- max speed of the users internet?
            -- minbitrate,  -- min speed of the users internet?
            maxextended,
            startdelay,  -- start delay in seconds for pre-roll, mid-roll, or post-roll ad placements

            maxduration,
            minduration,
            category,

            floor_price

        from triton_podcast_bids

    ),

    triton_podcast_bids_abridged_stats as (

        select
            triton_podcast_bids_abridged.*,

            case when gender is not null then 1 else 0 end as has_demo_data,
            case
                when content_genre is not null then 1 else 0
            end as has_content_genre_data,
            case
                when content_series is not null then 1 else 0
            end as has_content_series_data,
            case
                when content_language is not null then 1 else 0
            end as has_content_language_data,
            case when category is not null then 1 else 0 end as has_category_data

        from triton_podcast_bids_abridged

    ),

    triton_podcast_with_categories as (

        select *
        from triton_podcast_bids_abridged_stats as podcast
        left join
            dbt_paytunes_staging.stg_ad_category as ad_category
            on podcast.cat = ad_category.unique_id
    )

select *
from triton_podcast_with_categories
