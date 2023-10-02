with

    device_company as (
        select 
            id as company_id, 
            name as company_make
        from publify_raw.metadata_devicecompany_csv
    )

select *
from device_company
