with

    tags as (select * from {{ ref("stg_tags") }}),

    appmaster_tags_mapping as (select * from {{ ref("stg_appmaster_tags") }}),

    ssp_apps as (select * from {{ ref("int_ssp_apps") }}),

    appmaster_tags_merged as (

        select appmaster_tags_mapping.appmaster_id as publify_app_id, tags.tag_name
        from appmaster_tags_mapping
        left join tags on appmaster_tags_mapping.tags_id = tags.tag_id
    ),

    ssp_apps_tags as (

        select * from ssp_apps left join appmaster_tags_merged using (publify_app_id)
    )

select *
from
    ssp_apps_tags
    -- where publify_app_name is not null and tag_name is not null
    
