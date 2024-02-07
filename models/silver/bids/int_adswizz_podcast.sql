{%- set site_app_variables = [
    "cat",
    "publisher_name",
    "publisher_cat",
    "content_language",
    "content_cat",
    "content_genre",
    "content_id",
    "content_url",
    "content_title",
    "content_episode",
    "content_season",
    "content_series",
] -%}


with

    adswizz_podcast as (select * from {{ ref("stg_bids_adswizz_podcast_jan11") }}),

    adswizz_podcast_cleaned as (

        select

            device_language,
            case
                when lower(bid_device_os) in ('ios', 'android')
                then lower(bid_device_os)
                else 'other'
            end as device_os,

            site_page,

            {%- set site = "site_" %} {%- set app = "app_" %}
            {%- for variable in site_app_variables %}
                case
                    when platform_type = 'site'
                    then {{ site }}{{ variable }}
                    else {{ app }}{{ variable }}
                end as {{ variable }},
            {% endfor -%}

            imp_pmp_deals_0_bidfloorcur,
            imp_pmp_deals_0_bidfloor,
            imp_bidfloorcur,
            imp_bidfloor,

            imp_audio_stiched as stiched,
            imp_audio_maxseq as maxseq,
            imp_audio_startdelay as startdelay,
            imp_audio_maxduration as maxduration,
            imp_audio_minduration as minduration,

            imp_audio_companion_type_0,
            imp_audio_companion_type_1,
            imp_audio_companion_type_2,
            imp_audio_companion_ad_0_h,
            imp_audio_companion_ad_1_h,
            imp_audio_companion_ad_2_h,
            imp_audio_companion_ad_3_h,
            imp_audio_companion_ad_0_w,
            imp_audio_companion_ad_1_w,
            imp_audio_companion_ad_2_w,
            imp_audio_companion_ad_3_w,

            gender,
            yob,
            category,

            floor_currency,
            floor_price

        from adswizz_podcast

    ),

    adswizz_podcast_cleaned_stats as (

        select
            adswizz_podcast_cleaned.*,

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

        from adswizz_podcast_cleaned

    ),

    adswizz_podcast_with_categories as (

        select *
        from adswizz_podcast_cleaned_stats as podcast
        left join
            dbt_paytunes_staging.stg_ad_category as ad_category
            on podcast.cat = ad_category.unique_id
    )

select *
from adswizz_podcast_with_categories
