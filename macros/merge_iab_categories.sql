{% macro merge_iab_categories(bids, iab_categories) %}

    select
        bids.*,
        coalesce(c.category_name, d.category_name) as category_name,
        coalesce(c.tier_1_category, d.tier_1_category) as tier_1_category,
        coalesce(c.tier_2_category, d.tier_2_category) as tier_2_category,
        coalesce(c.tier_3_category, d.tier_3_category) as tier_3_category,
        coalesce(c.tier_4_category, d.tier_4_category) as tier_4_category,
        coalesce(c.itunes_category, d.itunes_category) as itunes_category
        
    from {{ bids }} as bids
    left join {{ iab_categories }} as c on bids.category = c.old_iab_category
    left join {{ iab_categories }} as d on bids.category = d.unique_id
{% endmacro %}
