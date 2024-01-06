with

    track as (select * from {{ ref("int_track_grouped") }}),

    pincodes as (
        select * from {{ ref("stg_pincode_metadata") }} where is_verified = 'true'
    ),

    device_os_metadata as (select * from {{ ref("stg_device_os_metadata") }}),

    ssp_apps as (select * from {{ ref("int_ssp_apps") }}),

    track_abridged as (

        select
            ssp,
            ad_type,
            platform_type,
            device_os,
            pincode,
            app_name,
            hour,
            sum(impression) as impression,
            sum(complete) as complete,
            sum(creative_view) as creative_view,
            sum(click) as click

        from track
        group by 1, 2, 3, 4, 5, 6, 7
    ),

    track_abridged_cleaned as (
        select
            ssp,
            ad_type,
            platform_type,
            device_os,
            pincode,
            app_name,
            hour,
            case
                when impression < creative_view
                then creative_view
                when impression < complete
                then complete
                else impression
            end as impression,
            complete,
            creative_view,
            click

        from track_abridged

    ),

    merged_with_pincodes as (
        select track.*, pincodes.city, pincodes.state

        from track_abridged_cleaned as track
        left join pincodes on track.pincode = pincodes.pincode
    ),

    final as (

        select
            ssp,
            ad_type,
            platform_type,
            device_os,
            app_name,
            city,
            state,
            hour,
            sum(impression) as impression,
            sum(complete) as complete,
            sum(creative_view) as creative_view,
            sum(click) as click

        from merged_with_pincodes
        where city is not null
        group by 1, 2, 3, 4, 5, 6, 7, 8

    )

select *
from final
where ssp is not null and ssp != 'offline'
