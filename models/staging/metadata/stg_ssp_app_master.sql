with

    ssp_app_master as (

        select

            appmaster_id as publify_app_id,
            appmaster_name as publify_app_name,
            appmaster_ssp_app_id as publify_app_master_id
            
        from paytunes_data.metadata_appmaster
    )

select *
from ssp_app_master
