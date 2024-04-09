with

    ssp_app_master as (

        select id as publify_app_id, name as publify_app_name

        from hive_metastore.paytunes_data.metadata_appmaster
    )

select *
from ssp_app_master
