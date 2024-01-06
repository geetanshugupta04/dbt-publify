with

    publishers as (
        select 
            id as app_id, 
            name as publisher_group, 
            publisher_id,
            ssp 
            from paytunes_data.metadata_publisher
    )

select *
from publishers
