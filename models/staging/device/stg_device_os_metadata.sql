with

    device_os as (
        select 
            cleaned_device_os, 
            device_os

        from publify_raw.metadata_deviceosmetadata_csv

    )

select *
from device_os
