with

    mctv_bids as (

        select
            *,
            case
                when
                    lower(app_publisher_name) ilike "%jio%"
                    or lower(app_app_name) ilike "%jio%"
                then "Jio"
                when
                    lower(app_publisher_name) ilike "%viki%"
                    or lower(app_app_name) ilike "%viki%"
                then "Viki"
                when
                    lower(app_publisher_name) ilike "%yupptv%"
                    or lower(app_app_name) ilike "%yupptv%"
                    or lower(app_publisher_name) ilike "%wtodt-pim0g%"
                then "Samsung YuppTV"
                when
                    lower(app_publisher_name) ilike "%tcl%"
                    or lower(app_app_name) ilike "%tcl%"
                then "TCL Channel"
                else "Other"
            end as publisher
        from {{ ref("stg_mctv_bids") }}
    ),

    pincodes as (
        select * from {{ ref("stg_pincode_metadata") }} where is_verified = 'true'
    ),

    ssp_apps as (select * from {{ ref("int_ssp_apps") }}),

    mctv_merged_with_pincodes as (
        select bids.*, pincodes.urban_or_rural, pincodes.city, pincodes.state

        from mctv_bids as bids
        left join pincodes on bids.pincode = pincodes.pincode
    ),

    clean_ssp_apps as (
        select
            ssp,
            platform_type,
            ssp_app_name,
            ssp_app_id,
            bundle,
            domain,
            publisher_id,
            publify_app_name,
            publisher_group

        from ssp_apps
    ),

    mctv_merged_with_pincodes_apps as (

        select
            clean_bids.*,
            -- apps.publify_publisher_id,
            apps.publify_app_name,
            apps.publisher_group

        from mctv_merged_with_pincodes as clean_bids
        left join
            clean_ssp_apps as apps
            on clean_bids.ssp = apps.ssp
            and clean_bids.platform_type = apps.platform_type
            and clean_bids.app_app_id = apps.ssp_app_id
            and clean_bids.app_app_name = apps.ssp_app_name
            and clean_bids.app_bundle = apps.bundle
            -- and clean_bids.appdomain = apps.domain
            and clean_bids.app_publisher_id = apps.publisher_id

    )

select *
from
    mctv_merged_with_pincodes_apps
    -- where is_verified = 'true'
    
