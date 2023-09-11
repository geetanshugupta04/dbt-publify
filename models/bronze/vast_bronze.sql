with

    vast as (
        select
            date(start_datetime) as date,
            ad_type,
            make,
            model,
            device_os,
            sum(count) as bids_won

        from {{ref("vast")}}
        group by date, ad_type, make, model, device_os

    )

select *
from vast
