with

    ssp_app_master as (

        select id as publify_app_id, name as publify_app_name

        from paytunes_data.metadata_appmaster
    )

select *
from ssp_app_master
