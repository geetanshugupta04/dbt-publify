with

    pincode_metadata as (

        select pincode, urban_or_rural, city, grand_city, state, is_verified
        from paytunes_data.metadata_pincodemetadata

    )

select *
from pincode_metadata
