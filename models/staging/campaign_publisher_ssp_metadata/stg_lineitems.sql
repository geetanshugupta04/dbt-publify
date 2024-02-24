with

    lineitems as (
        select
            id as line_item_id,
            line_item_name,
            line_item_type,
            c_line_item_name,
            c_line_item_type,
            campaign_id

        from paytunes_data.campaign_lineitem
        group by
            id,
            line_item_name,
            line_item_type,
            c_line_item_name,
            c_line_item_type,
            campaign_id

    )

select *
from lineitems
