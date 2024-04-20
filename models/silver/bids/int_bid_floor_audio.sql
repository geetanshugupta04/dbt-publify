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

    ssp_apps_tags as (select * from {{ ref("int_ssp_apps_tags") }}),

    ssp_publishers as (select * from {{ ref("int_ssp_publishers") }}),

    merged_with_ssp_apps_tags as (
        {{ merge_ssp_apps("merged_with_pincodes", "ssp_apps_tags") }}
    ),

    merged_with_ssp_publishers as (

        {{ merge_ssp_publishers("merged_with_ssp_apps_tags", "ssp_publishers") }}
    ),

    iab_categories as (select * from {{ ref("stg_ad_categories") }}),

    merged_with_iab_categories as (

        {{ merge_iab_categories("merged_with_ssp_publishers", "iab_categories") }}

    ),

    final as (

        select
            ssp,
            ad_type,

            device_os,
            cleaned_device_os,

            model,
            make,
            final_make,
            final_model,

            case when deal_0 is null then 'NA' else deal_0 end as deal_0,
            age,
            gender,

            pincode,
            urban_or_rural,
            city,
            grand_city,
            state,

            ssp_app_id,
            ssp_app_name,
            bundle,
            domain,
            publisher_id,

            publify_app_name,
            case
                when publify_ssp_publisher_name is null
                then lower(ssp_publisher_name)
                else lower(publify_ssp_publisher_name)
            end as publisher_final,

            app_category,
            category,
            category_name,

            minduration,
            maxduration,
            series,
            genre,

            case when floor_price is null then 0 else floor_price end as fp,
            date,
            bids

        from merged_with_iab_categories as bids
    )

select *
from
    final

    {#





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


#}
    
