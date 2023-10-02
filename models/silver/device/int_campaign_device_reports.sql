with

    campaign as (select * from {{ ref("stg_campaigns") }} where campaign_id = ''),

    lineitems as (

        select *
        from {{ ref("stg_lineitems") }}
        where campaign_id in (select campaign_id from campaign)
    ),

    track_device as (

        select *
        from
            {{ ref("stg_track_device") }}
            wnere line_item_id in (select line_item_id from lineitems)
    ),

    device_master as (select * from {{ ref("int_device_master") }})

    publishers as (select * from {{ ref("stg_publishers") }}),

    track as (
        select
            line_item_id,
            app_id,
            make,
            model,
            date,
            sum(impression) as impression,
            sum(creative_view) as creative_view,
            sum(click) as click,
            sum(complete) as complete
        from track_device_hourly
        where line_item_id in (select line_item_id from line_detail)
        group by line_item_id, app_id, make, model, date
        order by line_item_id, app_id, make, model, impression desc
    ),

    merged as (
        select
            t.*,
            p.publisher_group,
            p.ssp
            campaign.campaign_name,
            campaign.campaign_end_date,
            campaign.show_avg_cpm
        from track1 t
        left join publishers as p on t.app_id = p.app_id
        left join campaign on r.campaign_id = campaign.campaign_id

    ),

    -- business adjustments
    merged_with_adjustments as (
        select
            line_item_id,
            pincode,
            app_id,
            date,
            "Publisher Group",
            ssp,
            sum(impression) as impression,
            sum(click) as click,
            sum(
                case when ssp = 'offline' then impression else complete end
            ) as complete,
            sum(
                case when ssp = 'offline' then impression else creative_view end
            ) as creative_view
        from merged
        group by line_item_id, pincode, app_id, date, "Publisher Group", ssp
    ),

    final_merge_device_master as (
        select
            m2.*,
            d.model_name,
            d.company,
            d.release_month,
            d.release_year,
            d.cost,
            d.device_type
        from merged1 m2
        left join
            device_master d
            on upper(replace(m2.make, ' ', '')) = upper(replace(d.make, ' ', ''))
            and upper(replace(m2.model, ' ', '')) = upper(replace(d.model, ' ', ''))
    )

select *
from final_merge_device_master