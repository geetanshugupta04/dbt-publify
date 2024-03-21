with tags as (select id as tag_id, name as tag_name from paytunes_data.metadata_tags)

select *
from tags
