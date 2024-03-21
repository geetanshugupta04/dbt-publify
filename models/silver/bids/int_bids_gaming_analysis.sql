with

    bids as (select * from {{ ref("stg_bids_for_gaming_analysis") }}),

    ssp_apps_tags_mapping as (select * from {{ ref("int_ssp_apps_tags") }}),

    ad_categories as (select * from {{ ref("stg_ad_categories") }}),

    cleaned_bids as (

        select
            ssp,
            date,
            case
                when feed = 3 then 'podcast' when ad_type is null then 'NA' else ad_type
            end as ad_type,
            case
                when
                    device_os is null
                    or device_os in ('other', 'unknown', 'not found', 'na')
                then 'NA'
                when device_os ilike '%mac%' or device_os ilike '%os x%'
                then 'macos'
                when device_os ilike '%linux%'
                then 'linux'
                when device_os ilike '%windows%'
                then 'windows'
                when device_os ilike '%chrome%'
                then 'chromeos'

                else device_os
            end as device_os,
            make,
            model,
            case when dealcode is null then 'NA' else dealcode end as dealcode,
            ip,
            ifa,
            uid,
            app_id as ssp_app_id,
            app_name as ssp_app_name,
            bundle,
            publisher_id,
            publisher_name,
            split(category, ',')[0] as category,
            bid_count as bids

        from bids

    ),

    bids_with_apps_tags as (

        select bids.*, app_tags.publify_app_name, app_tags.tag_name as app_category
        from cleaned_bids as bids
        left join
            ssp_apps_tags_mapping as app_tags
            on bids.ssp_app_id = app_tags.ssp_app_id
            and bids.ssp_app_name = app_tags.ssp_app_name
            and bids.bundle = app_tags.bundle
            and bids.publisher_id = app_tags.publisher_id

    ),

    bids_with_tags_categories as (

        select bids.*, coalesce(c.category_name, d.category_name) as category_name
        from bids_with_apps_tags as bids
        left join ad_categories as c on bids.category = c.old_iab_category
        left join ad_categories as d on bids.category = d.iab_unique_id

    ),

    final as (

        select
            date,
            ssp,
            ad_type,
            device_os,
            make,
            model,
            dealcode,
            ip,
            ifa,
            uid,
            ssp_app_id,
            ssp_app_name,
            bundle,
            publisher_id,
            publisher_name,
            category,
            publify_app_name,
            case
                when app_category is null then 'NA' else app_category
            end as app_category_tag,
            case
                when category_name is null then 'NA' else category_name
            end as iab_category,
            bids
        from bids_with_tags_categories

    )

select *
from final
