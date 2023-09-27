with

    track_geo as (
        select
            line_item_id,
            coalesce(pincode, 'NA') as pincode,
            app_id,
            date,
            sum(impression) as impression,
            sum(creative_view) as creative_view,
            sum(click) as click,
            sum(complete) as complete
        from {{ ref("track_geo") }}
        where line_item_id in (select line_item_id from {{ ref("lineitems_specific") }})
        group by line_item_id, pincode, app_id, date
    )

select *
from track_geo
