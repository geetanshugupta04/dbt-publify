with

    ssp_app_metadata as (
        select
            
            ssp_app_id,
            name as ssp_app_name,
            ssp,
            bundle,
            publisher_id,
            app_id as publify_app_id

        from hive_metastore.paytunes_data.metadata_sspapp

    )
select *
from ssp_app_metadata
