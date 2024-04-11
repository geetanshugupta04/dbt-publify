with

    device_company as (
        select id as company_id, lower(trim(name)) as company_make
        from hive_metastore.paytunes_data.metadata_devicecompany
    )

select *
from device_company
