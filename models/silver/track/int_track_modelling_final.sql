with

    track as (
        select *
        from {{ ref("int_track_modelling_categories") }}

        where
            age != '13-17'
            and device_type = '1'
            and ad_type = 'audio'
            and platform_type = 'app'

    ),

    -- device_master as (select * from {{ ref("int_device_master") }}),
    -- merged_with_device_master as (
    -- select
    -- m.*,
    -- master.device_id,
    -- master.raw_make,
    -- master.raw_model,
    -- master.company_make,
    -- master.master_model,
    -- master.device_type,
    -- master.release_month,
    -- master.release_year,
    -- master.cost
    -- from track as m
    -- left join
    -- device_master as master
    -- on m.device_make = master.raw_make
    -- and m.device_model = master.raw_model
    -- )
    top_cities as (

        select city, sum(impression) as imp from track group by city having imp > 10000

    ),

    top_categories as (

        select category_name, sum(impression) as imp
        from track
        where category_name is not null
        group by 1
        having imp > 10000
    ),

    cleaned as (
        select
            track.*,

            case
                when device_make = 'apple' or device_model = 'iphone'
                then 'iphone'
                -- when device_model = 'android-generic'
                -- then 'android'
                -- when
                -- device_make in (
                -- 'oneplus',
                -- 'redmi',
                -- 'vivo',
                -- 'samsung',
                -- 'oppo',
                -- 'realme',
                -- 'xiaomi',
                -- 'poco',
                -- 'motorola',
                -- 'infinix'
                -- )
                -- then device_make
                else 'android'
            end as device_os,

            case
                when category_name in (select category_name from top_categories)
                then category_name
                else 'other'
            end as category_cleaned,

            case when age is null then 'unspecified' else age end as age_cleaned,

            case when gender is null then 'both' else gender end as gender_cleaned

        from track
        where city in (select city from top_cities)
    ),

    final as (
        select
            -- ad_type,
            -- platform_type,
            ssp,
            device_os,
            -- device_type,
            city,
            urban_or_rural,
            -- dealcode,
            age_cleaned,
            gender_cleaned,
            category_cleaned,
            app_id,
            domain,
            app_name,
            p_bundle,
            date,
            -- floor_currency,
            round(avg_floor_price, 4) as avg_floor_price,
            round(avg_bid_price, 4) as avg_bid_price,
            sum(impression) as impression,
            sum(complete) as complete,
            sum(click) as click
        from cleaned
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14
    )

select *
from final
