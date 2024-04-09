with

    device_os as (

        select cleaned_device_os, device_os
        from hive_metastore.paytunes_data.metadata_deviceosmetadata

    )

select *
from device_os
