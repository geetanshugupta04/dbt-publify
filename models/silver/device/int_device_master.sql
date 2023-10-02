with

    device_company as (select * from {{ ref("stg_device_company_metadata") }}),

    device_metadata as (select * from {{ ref("stg_device_make_model_metadata") }}),

    device_master as (select * from {{ ref("stg_device_master") }}),

    master_merge as (
        select
            master.device_id,
            master.company_id,
            device_raw.raw_make,
            device_raw.raw_model,
            company.company_make,
            master.master_model,
            master.device_type,
            master.release_month,
            master.release_year,
            master.cost

        from device_metadata as device_raw
        left join device_master as master on device_raw.device_id = master.device_id
        left join device_company as company on master.company_id = company.company_id
    )

select *
from master_merge
where lower(raw_make) = 'lava'
