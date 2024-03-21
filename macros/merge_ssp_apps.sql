{% macro merge_ssp_apps(bids, ssp_apps) %}

select bids.*, apps.publify_app_name

from {{ bids }} as bids
left join
    {{ ssp_apps }} as apps
    on bids.ssp_app_id = apps.ssp_app_id
    and bids.ssp_app_name = apps.ssp_app_name
    and bids.bundle = apps.bundle
    and bids.publisher_id = apps.publisher_id

    {% endmacro %}
