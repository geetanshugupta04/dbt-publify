with

    bids as (select * from {{ ref("int_device_os_merge") }}),

    pincodes as (select * from {{ ref("stg_pincode_metadata") }}),

    merged_with_device_os_pincodes as (
        select
            bids.*,
            pincodes.urban_or_rural,
            pincodes.city,
            pincodes.grand_city,
            pincodes.state,
            pincodes.is_verified
        from bids
        left join pincodes on bids.pincode = pincodes.pincode
    )

select *
from merged_with_device_os_pincodes
where is_verified = 'true'
