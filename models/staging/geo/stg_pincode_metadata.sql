with

    pincode_metadata as (

        select pincode, urban_or_rural, city, grand_city, state
        from publify_raw.metadata_pincodemetadata_csv

    )

    select * from pincode_metadata
