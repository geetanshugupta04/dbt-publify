with

    ssp_publisher_metadata as (

        select

            ssppublisher_publisher_id as ssp_publisher_id,
            ssppublisher_name as ssp_publisher_name,
            ssppublisher_ssp as ssp,
            ssppublisher_publisher_master_id as publify_ssp_publisher_id

        from hive_metastore.paytunes_data.metadata_ssppublisher

    )

select *
from ssp_publisher_metadata
-- where 
-- ssp_publisher_master_id not in ('NULL')
