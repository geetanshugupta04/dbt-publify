with

    track as (
        select *
        from {{ ref("int_track_grouped") }}
        where ssp is not null and ssp != 'offline'
    ),

    pincodes as (
        select * from {{ ref("stg_pincode_metadata") }} where is_verified = 'true'
    ),

    dealcodes as (select * from {{ ref("stg_dealcodes") }}),

    track_cleaned as (
        select
            ad_type,
            platform_type,
            ssp,

            device_make,
            device_model,
            device_type,

            pincode,
            dealcode,

            app_id,
            split_part(category, ',', 1) as category,
            domain,
            app_name,
            p_bundle,

            date,
            floor_currency,
            avg_floor_price,
            avg_bid_price,

            case
                when impression < creative_view
                then creative_view
                when impression < complete
                then complete
                else impression
            end as impression,
            complete,
            creative_view,
            click

        from track

    ),

    merged_with_pincodes as (
        select
            track.*,
            pincodes.city,
            -- pincodes.state,
            pincodes.urban_or_rural
        -- pincodes.grand_city
        from track_cleaned as track
        left join pincodes on track.pincode = pincodes.pincode
    ),

    -- select category, old_iab_category, iab_unique_id, category_name, count(*) from
    -- merged_with_pincodes_ad_categories
    -- group by 1,2,3, 4
    -- order by count(1) desc
    merged_with_pincodes_dealcodes as (
        select m.*, d.age, d.gender

        from merged_with_pincodes as m
        left join dealcodes as d on m.dealcode = d.deal_id

    ),

    -- select dealcode, age, gender, count(*) from
    -- merged_with_pincodes_ad_categories_dealcodes
    -- group by 1,2,3
    -- order by count(1) desc
    final as (

        select
            ad_type,
            platform_type,
            ssp,

            device_make,
            device_model,
            device_type,

            city,
            -- state,
            urban_or_rural,
            -- grand_city,
            dealcode,
            age,
            gender,

            category,

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
            -- sum(creative_view) as creative_view,
            sum(click) as click

        from merged_with_pincodes_dealcodes
        where city is not null
        group by
            1,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
            9,
            10,
            11,
            12,
            13,
            14,
            15,
            16,
            15,
            16,
            17,
            18,
            19,
            20
    ),

    ad_categories as (select * from {{ ref("stg_metadata_content_categories") }}),

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

        from final as t
        left join ad_categories as c on t.category = c.old_iab_category
        left join ad_categories as d on t.category = d.iab_unique_id

    ),

    final_2 as (
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
