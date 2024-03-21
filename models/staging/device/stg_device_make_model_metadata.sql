with

    device_metadata as (
        select device_id, make as raw_make, model as raw_model
        from paytunes_data.metadata_devicemetadata
    )

select *
from device_metadata
