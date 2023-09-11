with

    lineitems as (
        select
            id as line_item_id,
            line_item_name,
            line_item_type,
            c_line_item_type as c_line_item_type,
            campaign_id

        from campaign_lineitem_csv

    )

select *
from lineitems
