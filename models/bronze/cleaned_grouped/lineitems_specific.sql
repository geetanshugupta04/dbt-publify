with
    lineitems as (
        select *
        from {{ ref("lineitems") }}
        where campaign_id in (select campaign_id from {{ ref("campaigns_specific") }})

    )

select *
from lineitems
