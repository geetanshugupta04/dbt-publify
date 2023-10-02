with

    device_master as (
        select
            id as device_id,
            company_id,
            model_name as master_model,
            device_type,
            release_month,
            release_year,
            cost
        from publify_raw.metadata_devicemaster_csv
    )

select *
from device_master
