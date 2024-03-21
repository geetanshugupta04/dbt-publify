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

        from paytunes_data.metadata_devicemaster
    )

select *
from device_master
