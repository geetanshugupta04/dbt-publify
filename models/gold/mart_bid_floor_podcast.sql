{% set health_223 = [
    "ASMR Rain Recordings",
    "7 Good Minutes",
    "Hello Doctor",
    "Motivation Daily by Motiversity",
    "Pyaar, Poetry Etc. with Shradha",
] %}
{% set comedy_348 = [
    "Asatha Povathu Yaru | Nakkheeran Studio",
    "Teen Taal",
] %}

{% set religion_361 = [
    "Art of Living with Gurudev",
    "Ayyappa Kannada",
    "Ayyappa Malayalam",
    "Ayyappa Tamil",
    "Ayyappa Telugu",
    "Bangla Waz",
    "Bhagavad Gita",
    "Bhagavad Gita Hindi",
    "Devi Katha",
    "Islamic Bayan",
    "Teerthsthal",
    "Ramayan Hindi",
    "Ramayan Kannada",
    "Ramayan Telugu",
    "Osho Hindi Podcast",
    "Learn About Islam",
    "Mere Prabhu Ram",
    "Shiva - Narrated by Jackie Shroff",
    "Shivgatha",
    "The Stories of Mahabharata",
] %}

{% set sports_370 = ["F1: Beyond The Grid", "IPL Ki Tein Tein"] %}

{% set fiction_48 = [
    "Sunday Suspense (Full Episodes)",
    "Storybox with Jamshed Qamar Siddiqui",
    "Goppo mir-er Thek",
    "Stories with Abhash Jha",
    "Real Ghost Stories Online",
] %}

{% set tech_596 = ["Tech Tonic with Munzir", "Sabka Maalik Tech"] %}

{% set news_374 = [
    "3 Things",
    "5 Minute",
    "Aaj Ke Akhbaar",
    "Din Bhar",
    "Fact Check",
    "ThePrint",
] %}


{% set leisure_239 = ["Gyaan Dhyaan", "Iti Itihaas", "Naami Giraami"] %}

{% set society_and_culture_186 = [
    "Tumne Kisi Se Kabhi Pyaar Kiya Hai?",
] %}


with

    podcast_bids as (select * from {{ ref("int_bid_floor_podcast") }}),

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

            case
                when publify_app_name is null then publisher_final else publify_app_name
            end as publify_app_final,

            case
                when publisher_final ilike 'grupo%'
                then 'grupo'
                when publisher_final ilike '%nzme%'
                then 'nzme'
                when publisher_final ilike '%observador%'
                then 'observador'
                else publisher_final
            end as publisher_final,

            case
                when app_category_tag is null then 'NA' else app_category_tag
            end as app_category_tag,
            case
                {% for series in health_223 %}

                    when series = '{{ series }}' then '223'
                {% endfor %}
                {% for series in comedy_348 %}

                    when series = '{{ series }}' then '348'
                {% endfor %}
                {% for series in religion_361 %}

                    when series = '{{ series }}' then '361'
                {% endfor %}
                {% for series in sports_370 %}

                    when series = '{{ series }}' then '370'
                {% endfor %}
                {% for series in fiction_48 %}

                    when series = '{{ series }}' then '48'
                {% endfor %}
                {% for series in tech_596 %}

                    when series = '{{ series }}' then '596'
                {% endfor %}
                {% for series in news_374 %}

                    when series = '{{ series }}' then '374'
                {% endfor %}
                {% for series in leisure_239 %}

                    when series = '{{ series }}' then '239'
                {% endfor %}
                when series = 'DHADKANE MERI SUN'
                then '353'
                else category
            end as category,

            -- iab_category_name,
            -- case when tier_1_category is null then 'NA' else tier_1_category end as
            -- tier_1_category,
            -- case when tier_2_category is null then 'NA' else tier_2_category end as
            -- tier_2_category,
            -- case when tier_3_category is null then 'NA' else tier_3_category end as
            -- tier_3_category,
            -- case when tier_4_category is null then 'NA' else tier_4_category end as
            -- tier_4_category,
            -- case when itunes_category is null then 'NA' else itunes_category end as
            -- itunes_category,
            minduration,
            maxduration,
            startdelay,
            maxextended,
            maxseq,
            stitched,

            series,
            genre,

            case when fp = 99999 then 1 else 0 end as null_fps,

            fp,
            sum(bids) as bids

        from podcast_bids
        -- where publisher_final not in ('tim media', 'light fm')
        group by all

    ),

    iab_categories as (select * from {{ ref("int_iab_content_categories") }}),

    merged_with_iab_categories as (

        select
            bids.*,
            d.category_name as iab_category_name,
            d.tier_1_category as tier_1_category,
            d.tier_2_category as tier_2_category,
            d.tier_3_category as tier_3_category,
            d.tier_4_category as tier_4_category,
            case
                when d.category_name = 'Healthy Living'
                then 'Health & Fitness'
                else d.itunes_category
            end as itunes_category

        from cleaned_bids as bids
        left join iab_categories as d on bids.category = d.unique_id
    ),

    big_podcasts as (
        select
            bids.*,
            sum(bids) over (partition by ssp) as ssp_bids_sum,
            sum(bids) over (partition by ssp, null_fps) as ssp_bids_sum_fp_not_null,
            sum(bids) over (partition by itunes_category) as itunes_bids_sum,
            sum(bids) over (partition by ssp, itunes_category) as ssp_itunes_bids_sum,
            sum(bids) over (
                partition by itunes_category, null_fps
            ) as itunes_bids_sum_fp_not_null,
            sum(bids) over (partition by iab_category_name) as iab_bids_sum,
            sum(bids) over (
                partition by iab_category_name, null_fps
            ) as iab_bids_sum_fp_not_null,
            sum(bids) over (partition by ad_type, series) as series_sum,
            sum(bids) over (partition by series, null_fps) as series_sum_fp_not_null,

            sum(bids) over (partition by ad_type, deal_0) as deal_bids_sum,
            sum(bids) over (
                partition by ad_type, deal_0, publisher_id, null_fps
            ) as deal_pub_bids_sum,
            sum(bids) over (partition by ad_type, publify_app_final) as app_bids_sum

        from merged_with_iab_categories as bids
        qualify series_sum > 5000 and category is not null
    )

{#

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

    #}
select *
from big_podcasts
