with

    ad_categories as (

        select
            unique_id as iab_unique_id,
            name as category_name,
            old_iab_taxonomy as old_iab_category

        from paytunes_data.metadata_ad_category

    )

select *
from ad_categories
