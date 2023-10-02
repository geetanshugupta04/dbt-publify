with

device_metadata as (
  select
    device_id,
    make as raw_make,
    model as raw_model
  from
    publify_raw.metadata_devicemetadata_csv
)

select * from device_metadata