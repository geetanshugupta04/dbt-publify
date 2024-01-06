with

    bids as (select * from {{ ref("int_pincode_merge") }}),

    ssp_apps as (select * from {{ ref("int_ssp_apps") }}),

    clean_bids as (

        select

            ssp,
            case when feed = '3' then 'podcast' else ad_type end as ad_type,
            platform_type,
            feed,
            ssp_app_id,
            ssp_app_name,
            publisher_id,
            bundle,
            domain,
            banner_height,
            banner_width,
            min_duration,
            max_duration,
            cleaned_device_os_2 as cleaned_device_os,
            city,
            state,
            bid_count,
            avg_floor_price

        from bids
    ),

    clean_ssp_apps as (
        select
            ssp,
            platform_type,
            ssp_app_name,
            ssp_app_id,
            bundle,
            domain,
            publisher_id,
            publify_app_name,
            publisher_group

        from ssp_apps
    ),

    merged as (

        select
            clean_bids.*,
            -- apps.publify_publisher_id,
            apps.publify_app_name,
            apps.publisher_group

        from clean_bids
        left join
            clean_ssp_apps as apps
            on clean_bids.ssp = apps.ssp
            and clean_bids.platform_type = apps.platform_type
            and clean_bids.ssp_app_id = apps.ssp_app_id
            and clean_bids.ssp_app_name = apps.ssp_app_name
            and clean_bids.bundle = apps.bundle
            and clean_bids.domain = apps.domain
            and clean_bids.publisher_id = apps.publisher_id

    ),

    cleaned_pubs as (
        select
            merged.*,
            case

                when ssp = 'Triton'
                then ssp_app_name

                when bundle ilike '%pocketfm%' or domain ilike '%pocketfm%'
                then 'pocketfm'
                when
                    bundle ilike '%jiocinema%'
                    or domain ilike '%jiocinema%'
                    or ssp_app_id ilike '%jiocinema%'
                    or ssp_app_name ilike '%jiocinema%'
                then 'jiocinema'
                when
                    bundle ilike '%zee5%'
                    or domain ilike '%zee5%'
                    or ssp_app_id ilike '%zee5%'
                    or ssp_app_name ilike '%zee5%'
                then 'zee5'
                when
                    bundle ilike '%zeenews%'
                    or domain ilike '%zeenews%'
                    or ssp_app_id ilike '%zeenews%'
                    or ssp_app_name ilike '%zeenews%'
                then 'zeenews'
                when
                    bundle ilike '%zeenews%'
                    or domain ilike '%zeenews%'
                    or ssp_app_id ilike '%zeenews%'
                    or ssp_app_name ilike '%zeenews%'
                then 'zeenews'
                when
                    bundle ilike '%zeebiz%'
                    or domain ilike '%zeebiz%'
                    or ssp_app_id ilike '%zeebiz%'
                    or ssp_app_name ilike '%zeebiz%'
                then 'zeebiz'
                when
                    bundle ilike '%aljazeera%'
                    or domain ilike '%aljazeera%'
                    or ssp_app_id ilike '%aljazeera%'
                    or ssp_app_name ilike '%aljazeera%'
                then 'aljazeera'
                when
                    bundle ilike '%aljazeera%'
                    or domain ilike '%aljazeera%'
                    or ssp_app_id ilike '%aljazeera%'
                    or ssp_app_name ilike '%aljazeera%'
                then 'aljazeera'
                when
                    bundle ilike '%spreaker%'
                    or domain ilike '%spreaker%'
                    or ssp_app_id ilike '%spreaker%'
                    or ssp_app_name ilike '%spreaker%'
                then 'spreaker'
                when
                    bundle ilike '%radiomirchi%'
                    or domain ilike '%radiomirchi%'
                    or ssp_app_id ilike '%radiomirchi%'
                    or ssp_app_name ilike '%radiomirchi%'
                then 'radiomirchi'
                when
                    publisher_id in (
                        'tunein',
                        'zenomedia',
                        'jiosaavn',
                        'zenomedia',
                        'pocketfm',
                        'audiomob',
                        'tunein',
                        'trinityaudio',
                        'thedailywire',
                        'spreaker',
                        'simplecast',
                        'openstream',
                        'gadsme',
                        'empirestreaming',
                        'blogtalkradio',
                        'audiomob',
                        'audiomack',
                        'audioboom',
                        'advertisecast',
                        'acast'
                    )
                then lower(publisher_id)

                when ssp = 'Triton' and bundle ilike '%radiomirchi'
                then 'radiomirchi'
                when ssp = 'Triton' and bundle ilike '%radiomirchi'
                then 'radiomirchi'

                else publify_app_name
            end as publisher_cleaned

        from merged
    ),

    cleaned_pubs_2 as (

        select
            ssp,
            ad_type,
            platform_type,
            ssp_app_id,
            ssp_app_name,
            publisher_id,
            bundle,
            domain,
            publisher_cleaned,
            case
                when ssp = 'Rubicon' and publify_app_name is null
                then ssp_app_name
                else publify_app_name
            end as publify_app_name,
            banner_height,
            banner_width,
            min_duration,
            max_duration,
            cleaned_device_os,
            city,
            state,
            bid_count,
            avg_floor_price

        from cleaned_pubs

    )

select *
from cleaned_pubs_2
