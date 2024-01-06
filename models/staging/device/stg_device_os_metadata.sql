with

    device_os as (
        select cleaned_device_os, device_os from paytunes_data.metadata_deviceosmetadata

    )

select *
from device_os
