with

    bids as (select * from {{ ref("stg_bid_floor_audio") }}),

    device_os_metadata as (select * from {{ ref("stg_device_os_metadata") }}),

    merged_with_device_os as (

        {{ merge_cleaned_device_os("bids", "device_os_metadata") }}

    ),

    device_data as (select * from {{ ref("int_device_master") }}),

    merged_with_device_data as (

        {{ merge_device_data("merged_with_device_os", "device_data") }}
    ),

    dealcodes as (select * from {{ ref("stg_dealcodes") }}),

    merged_with_dealcodes as (
        {{ merge_dealcodes("merged_with_device_data", "dealcodes") }}
    ),

    pincodes as (select * from {{ ref("stg_pincode_metadata") }}),

    merged_with_pincodes as ({{ merge_pincodes("merged_with_dealcodes", "pincodes") }}),

    ssp_apps as (select * from {{ ref("int_ssp_apps") }}),

    ssp_publishers as (select * from {{ ref("int_ssp_publishers") }}),

    merged_with_ssp_apps as ({{ merge_ssp_apps("merged_with_pincodes", "ssp_apps") }}),

    merged_with_ssp_apps_publishers as (

        {{ merge_ssp_publishers("merged_with_ssp_apps", "ssp_publishers") }}
    ),

    cleaned_pubs_triton as ({{ clean_triton_pubs("merged_with_ssp_apps_publishers") }}),

    cleaned_pubs_adswizz as (
        {{ clean_adswizz_pubs("merged_with_ssp_apps_publishers") }}

    ),

    cleaned_pubs_union as (

        select ssp, ssp_app_name, publisher_cleaned
        from cleaned_pubs_triton
        union all
        select ssp, ssp_app_name, publisher_cleaned
        from cleaned_pubs_adswizz
    ),

    joined as (

        select
            bids.*,
            pubs.publisher_cleaned,
            case
                when bids.publify_ssp_publisher_name is null
                then pubs.publisher_cleaned
                else lower(bids.publify_ssp_publisher_name)
            end as publisher_final

        from merged_with_ssp_apps_publishers as bids
        left join
            cleaned_pubs_union as pubs
            on bids.ssp = pubs.ssp
            and bids.ssp_app_name = pubs.ssp_app_name

    )

select *
from joined
