with

    track as (select * from {{ ref("int_track_modelling_final") }}),

    final as (

        select
            -- domain,
            -- p_bundle,
            -- app_name,
            urban_or_rural,
            city,
            device_os,
            age_cleaned,
            gender_cleaned,
            -- date, -- 10 tuesday 14-15 weekend
            -- round(avg_floor_price, 2),
            round(avg(cast(avg_floor_price as float)), 4) as fp,
            round(avg(cast(avg_bid_price as float)), 4) as bp,
            round((sum(complete) / sum(impression)) * 100, 2) as ltr,
            round((sum(click) / sum(impression)) * 100, 2) as ctr,
            sum(complete) as completes,
            sum(click) as clicks,
            sum(impression) as impressions
        from dbt_paytunes_silver.int_track_modelling_final
        where
            domain in (
                'wynk.in'  -- 'odeeo.io',
            -- 'audiomob.com',
            -- 'pocketfm.com',
            -- 'Spotify.com'
            )
            and age_cleaned not in ('35-44', '45+')
            and device_os in (
                'android', 'iphone'
            -- 'NA',
            -- 'na',
            -- 'something',
            -- 'anything'
            )
            and avg_floor_price between 1 and 1.3
        group by 1, 2, 3, 4, 5
        having impressions > 1000
        order by city, ltr desc, impressions desc

    )

select *
from final
