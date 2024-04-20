{% macro merge_iab_categories(bids, iab_categories) %}

    select bids.*, coalesce(c.category_name, d.category_name) as category_name
    from {{ bids }} as bids
    left join {{ iab_categories }} as c on bids.category = c.old_iab_category
    left join {{ iab_categories }} as d on bids.category = d.iab_unique_id
{% endmacro %}
