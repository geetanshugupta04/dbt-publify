with

    device_company as (select * from {{ ref("stg_device_company_metadata") }}),

    device_metadata as (select * from {{ ref("stg_device_make_model_metadata") }}),

    device_master as (select * from {{ ref("stg_device_master") }}),

    device_master as (
        select
            master.id as device_id,
            master.company_id,
            device.make as device_make,
            device.model as device_model,
            company.make as company_make,
            master.model_name as master_model,
            master.device_type,
            master.release_month,
            master.release_year,
            master.cost
        from device_master as master
        left join device_company as company on master.company_id = company.company_id
        left join device_metadata on master.id = device.device_id
    )

    select * from device_master
