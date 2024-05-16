with

    iab_taxonomy_2point2 as (

        select
            cast(unique_id as int) as unique_id,
            name as category_name,
            tier_1 as tier_1_category,
            tier_2 as tier_2_category,
            tier_3 as tier_3_category,
            tier_4 as tier_4_category,
            extension

        from hive_metastore.paytunes_data.iab_taxonomy

    )

select *
from iab_taxonomy_2point2
