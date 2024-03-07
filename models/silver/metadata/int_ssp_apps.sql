with

    ssp_app_metadata as (select * from {{ ref("stg_ssp_app_metadata") }}),

    ssp_app_master as (select * from {{ ref("stg_ssp_app_master") }}),

    ssp_apps as (

        select
            metadata.ssp as ssp,
            metadata.ssp_app_name as ssp_app_name,
            metadata.ssp_app_id as ssp_app_id,
            metadata.bundle as bundle,
            master.publify_app_id,
            master.publify_app_name,
            master.publify_app_master_id

        from ssp_app_metadata as metadata
        left join ssp_app_master as master using (publify_app_id)
    )

select *
from ssp_apps
