with

    track as (select * from {{ ref("int_track_for_modelling_2") }}),

    ad_categories as (select * from {{ ref("stg_ad_categories") }}),

    track_with_categories as (
        select

            -- c.old_iab_category,
            -- c.iab_unique_id,
            -- case
            -- when m.category = 'IAB1'
            -- then 'Arts & Entertainment'
            -- when m.category = 'IAB24'
            -- then 'Uncategorized'
            -- when m.category = 'IAB12'
            -- then 'News'
            -- when m.category = 'IAB14'
            -- then 'Society'
            -- else c.category_name
            -- end as category_name
            t.*, coalesce(c.category_name, d.category_name) as category_name

        from track as t
        left join ad_categories as c on t.category = c.old_iab_category
        left join ad_categories as d on t.category = d.iab_unique_id

    ),

    final as (
        select

            ad_type,
            platform_type,
            ssp,

            lower(device_make) as device_make,
            lower(device_model) as device_model,
            device_type,

            city,
            urban_or_rural,
            dealcode,
            age,
            gender,

            category_name,
            app_id,
            domain,
            app_name,
            p_bundle,

            date,
            floor_currency,
            avg_floor_price,
            avg_bid_price,

            sum(impression) as impression,
            sum(complete) as complete,
            sum(click) as click

        from track_with_categories
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20

    )

select *
from final
