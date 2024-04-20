{% macro merge_ssp_apps(bids, ssp_apps_tags) %}

    select bids.*, apps.publify_app_name, apps.tag_name as app_category

    from {{ bids }} as bids
    left join
        {{ ssp_apps_tags }} as apps
        on bids.ssp_app_id = apps.ssp_app_id
        and bids.ssp_app_name = apps.ssp_app_name
        and bids.bundle = apps.bundle
        and bids.publisher_id = apps.publisher_id

{% endmacro %}
