with impressions as (

    select * from {{ ref('stg_impressions')}}
),
clicks as (

    select * from {{ ref('stg_clicks')}}
),

joined as (

    select *
    from impressions
    left join clicks
    using(auction_id)

)

select * from joined