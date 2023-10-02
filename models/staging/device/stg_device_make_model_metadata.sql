with

device_metadata as (
  select
    device_id,
    make as device_make,
    model as device_model
  from
    publify_raw.metadata_devicemetadata_csv
)

select * from device_metadata