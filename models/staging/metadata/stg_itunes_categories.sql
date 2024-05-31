with

    itunes_categories as (

        select
            cast(unique_id as int) as iab_unique_id,
            iab_category_name as category_name,
            itunes_category,
            itunes_subcategory

        from hive_metastore.paytunes_data.itunes_categories

    )

select *
from itunes_categories
