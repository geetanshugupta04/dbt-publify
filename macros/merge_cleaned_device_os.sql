{% macro merge_cleaned_device_os(bids, device_os_metadata) %}

    select
        bids.*,
        case
            when device_os_metadata.cleaned_device_os is null
            then device_os_metadata.device_os
            else device_os_metadata.cleaned_device_os
        end as cleaned_device_os

    from {{ bids }} as bids
    left join
        {{ device_os_metadata }} as device_os_metadata
        on bids.device_os = device_os_metadata.device_os

{% endmacro %}
