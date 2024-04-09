with appmaster_tags_mapping as (select * from hive_metastore.paytunes_data.metadata_appmaster_tags)

select *
from appmaster_tags_mapping
