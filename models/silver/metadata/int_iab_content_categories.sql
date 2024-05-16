with

    metadata_content_categories as (
        select * from {{ ref("stg_metadata_content_categories") }}
    ),

    iab_taxonomy as (select * from {{ ref("stg_iab_taxonomy_content_categories") }}),

    itunes_categories as (select * from {{ ref("stg_itunes_categories") }}),

    final as (

        select
            metadata.old_iab_category, iab_taxonomy.*, itunes_categories.itunes_category

        from metadata_content_categories as metadata
        left join iab_taxonomy on metadata.iab_unique_id = iab_taxonomy.unique_id
        left join
            itunes_categories
            on metadata.iab_unique_id = itunes_categories.iab_unique_id
        order by unique_id

    )

select *
from final
