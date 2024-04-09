with

    pincode_metadata as (

        select pincode, urban_or_rural, city, grand_city, state
        from hive_metastore.paytunes_data.metadata_pincodemetadata
        where is_verified = 'true'

    )

select *
from pincode_metadata
