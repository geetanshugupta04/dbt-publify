{% macro merge_device_data(bids, device_data) %}

    select
        bids.*,
        case
            when device_data.company_make is null
            then bids.make
            else device_data.company_make
        end as final_make,
        case
            when device_data.master_model is null
            then bids.model
            else device_data.master_model
        end as final_model,
        device_data.cost
    from {{ bids }} as bids
    left join
        {{ device_data }} as device_data
        on bids.make = device_data.raw_make
        and bids.model = device_data.raw_model

{% endmacro %}
