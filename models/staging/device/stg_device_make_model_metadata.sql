with

    device_metadata as (

        select device_id, lower(trim(make)) as raw_make, lower(trim(model)) as raw_model
        from hive_metastore.paytunes_data.metadata_devicemetadata
        group by 1,2,3

    )

select *
from device_metadata
