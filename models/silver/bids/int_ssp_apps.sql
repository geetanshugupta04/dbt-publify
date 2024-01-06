with

    ssp_app_metadata as (select * from {{ ref("stg_ssp_app_metadata") }}),

    ssp_app_master as (select * from {{ ref("stg_ssp_app_master") }}),

    publishers as (select * from {{ ref("stg_publishers") }}),

    ssp_apps as (

        select
            metadata.ssp as ssp,
            metadata.platform_type as platform_type,
            metadata.ssp_app_name as ssp_app_name,
            metadata.ssp_app_id as ssp_app_id,
            metadata.bundle as bundle,
            metadata.domain as domain,
            metadata.publisher_id as publisher_id,
            master.publify_app_id,
            master.publify_app_name

        from ssp_app_metadata as metadata
        left join ssp_app_master as master using (publify_app_id)
    ),

    ssp_apps_with_publishers as (

        select ssp_apps.*, publishers.publisher_group
        from ssp_apps
        left join publishers using (publisher_id)
    )

select *
from
    ssp_apps_with_publishers
    -- where ssp = 'adswizz'
    
