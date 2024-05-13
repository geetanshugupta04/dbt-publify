with

    top_podcasts as (

        select podcast, ssp, ssp_category

        from hive_metastore.paytunes_data.top_podcasts
        group by all

    )

select *
from top_podcasts
