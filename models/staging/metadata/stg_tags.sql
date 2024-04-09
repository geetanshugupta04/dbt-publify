with tags as (select id as tag_id, name as tag_name from hive_metastore.paytunes_data.metadata_tags)

select *
from tags
