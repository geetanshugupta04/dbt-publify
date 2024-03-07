with

    ssp_publisher_metadata as (select * from {{ ref("stg_ssp_publisher_metadata") }}),

    ssp_publisher_master as (select * from {{ ref("stg_ssp_publisher_master") }}),

    ssp_publishers as (
        select
            pub_meta.ssp_publisher_id,
            pub_meta.ssp_publisher_name,
            pub_meta.ssp,
            pub_master.publify_ssp_publisher_id,
            pub_master.publify_ssp_publisher_name,
            pub_master.publify_ssp_publisher_master_id

        from ssp_publisher_metadata as pub_meta
        left join ssp_publisher_master as pub_master using (publify_ssp_publisher_id)
    )

select *
from ssp_publishers
