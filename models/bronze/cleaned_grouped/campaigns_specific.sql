with
    campaigns as (
        select *

        from {{ ref("campaigns") }}
        where campaign_id = '6rK9oVszX74wce9i5sxWfg'

    )

select *
from campaigns
