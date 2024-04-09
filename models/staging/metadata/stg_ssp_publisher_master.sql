with

    ssp_publisher_master as (

        select

            publishermaster_id as publify_ssp_publisher_id,
            publishermaster_name as publify_ssp_publisher_name,
            publishermaster_ssp_publisher_id as publify_ssp_publisher_master_id

        from hive_metastore.paytunes_data.metadata_publishermaster

    )

select *
from ssp_publisher_master
