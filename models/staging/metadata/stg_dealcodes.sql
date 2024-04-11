with
    dealcodes as (

        select deal_id, age, gender, max(id) as id

        from hive_metastore.paytunes_data.metadata_dealcode
        group by 1, 2, 3

    )

select *
from dealcodes
