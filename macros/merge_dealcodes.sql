{% macro merge_dealcodes(bids, dealcodes) %}

    select
        bids.*,
        case when dealcodes.age is null then 'no deal' else dealcodes.age end as age,
        case
            when dealcodes.gender is null then 'no deal' else dealcodes.gender
        end as gender

    from merged_with_device_data as bids
    left join dealcodes as dealcodes on bids.deal_0 = dealcodes.deal_id
{% endmacro %}
