with

    publishers as (
        select 
            id as app_id, 
            name as publisher_group, 
            ssp 
            from publify_raw.metadata_publisher_csv
    )

select *
from publishers
