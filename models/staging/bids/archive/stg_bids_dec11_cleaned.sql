with

    raw_bids as (select * from {{ ref("stg_bids_dec11") }}),

    bids as (select 
        max_duration[0], count(*)
    
    
    from raw_bids
    group by 1

    order by max_duration 
    )

select *
from bids
