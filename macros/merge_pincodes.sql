{% macro merge_pincodes(bids, pincodes) %}

    select
        bids.*,
        pincodes.urban_or_rural,
        pincodes.city,
        pincodes.grand_city,
        pincodes.state

    from {{ bids }} as bids
    left join {{ pincodes }} as pincodes on bids.pincode = pincodes.pincode

{% endmacro %}
