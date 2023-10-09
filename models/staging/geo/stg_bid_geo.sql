with

    bids as (
        select
            date(start_datetime) as date,
            ad_type,
            coalesce(pincode, 'NA') as pincode,
            sum(count) as bid_requests,
            sum(case when bid_status = true then count else 0 end) as bid_response

        from publify_raw.bid_geo_hourly
        where country in ('IND', 'IN', 'in')
        -- and ad_type in 
        group by date, ad_type, pincode

    )

select *
from bids

