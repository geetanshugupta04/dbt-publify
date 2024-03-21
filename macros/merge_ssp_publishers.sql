{% macro merge_ssp_publishers(bids, ssp_publishers) %}

    select

        bids.*,
        ssp_publisher_id,
        ssp_publisher_name,
        publify_ssp_publisher_name,
        publify_ssp_publisher_master_id

    from {{ bids }} as bids
    left join
        {{ ssp_publishers }} as pubs
        on bids.publisher_id = pubs.ssp_publisher_id
        and bids.ssp = pubs.ssp

{% endmacro %}
