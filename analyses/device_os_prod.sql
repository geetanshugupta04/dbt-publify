/* Campaign X Device OS Summary */
/* Take out campaign data and campaign_id level*/
with
    camp_detail as (
        select
            id as campaign_id,
            name as campaign_name,
            min(start_date) as campaign_start_date,
            max(end_date) as campaign_end_date
        from campaign_campaign
        where id = '0'
        group by campaign_id, campaign_name
    ),
    /* Take out line item data at line_item_id level*/
    line_detail as (
        select
            id as line_item_id,
            line_item_name as line_item_name,
            line_item_type as line_item_type,
            c_line_item_type as c_line_item_type,
            campaign_id as campaign_id
        from campaign_lineitem
        where campaign_id in (select campaign_id from camp_detail)
        group by
            line_item_id, line_item_name, line_item_type, c_line_item_type, campaign_id
    ),
    /* Take out impression data*/
    track as (
        select
            line_item_id,
            app_id,
            dealcode,
            date,
            sum(impression) as impression,
            sum(creative_view) as creative_view,
            sum(click) as click,
            sum(complete) as complete
        from track_device_hourly
        where line_item_id in (select line_item_id from line_detail)
        group by line_item_id, app_id, dealcode, date
    ),

    deals as (select * from metadata_dealid where is_active is true),

    track_deal_merge as (
        select
            track.*,
            deals.line_item_type,
            deals.deal_type,
            deals.deal_id,
            deals.age,
            deals.gender

        from track
        left join deals on track.dealcode = deals.deal_id

    ),

    track1 as (
        select
            line_item_id,
            dealcode,
            app_id,
            date,
            age,
            gender,
            sum(impression) as impression,
            sum(creative_view) as creative_view,
            sum(click) as click,
            sum(complete) as complete
            

        from track_deal_merge
        group by 1,2,3,4,5,6,7
        

    ),
    /* Take out Publisher data*/
    pub_detail as (
        select id as app_id, name as publisher_group, ssp as ssp
        from metadata_publisher
    ),
    track_demo_pub as (
        select
            t.line_item_id,
            t.age,
            t.gender,
            t.date,
            t.impression,
            t.creative_view,
            t.click,
            t.complete,
            pub_detail.publisher_group,
            pub_detail.ssp
        from track1 t
        left join pub_detail on t.app_id = pub_detail.app_id
    ),

    track2 as (
        select
            line_item_id,
            age,
            gender,
            date,
            publisher_group,
            ssp,
            sum(impression) as impression,
            sum(click) as click,
            sum(case when ssp = 'offline' then impression else complete end) as complete
            ,
            sum(
                case when ssp = 'offline' then impression else creative_view end
            ) as creative_view
        from merged
        group by line_item_id, age,
            gender, publisher_group, ssp, date
    ),
    /* Change creative view data for spotify,saavn publisher*/
    track3 as (
        select
            line_item_id,
            age,
            gender,
            date,
            publisher_group,
            ssp,
            sum(impression) as impression,
            sum(click) as click,
            sum(complete) as complete,
            sum(
                case
                    when lower(publisher_group) in ('spotify', 'saavn')
                    then impression
                    else creative_view
                end
            ) as creative_view
        from track2
        group by line_item_id, age,
            gender, publisher_group, ssp, date
    ),
    /* Merge Line item metadata to line item- pub level data*/
    raw as (
        select
            t3.*,
            line_detail.campaign_id,
            line_detail.line_item_name,
            line_detail.line_item_type,
            line_detail.c_line_item_type
        from track3 t3
        left join line_detail on t3.line_item_id = line_detail.line_item_id
    ),
    /* Change data for Triton Audio Resso*/
    track_triton as (
        select
            line_item_id,
            line_item_type,
            c_line_item_type,
            campaign_id,
            age,
            gender,
            date,
            sum(impression) as impression,
            sum(click) as click,
            sum(complete) as complete,
            sum(
                case
                    when
                        lower(c_line_item_type)
                        in ('audio', 'audio_with_companion', 'video')
                    then impression
                    else creative_view
                end
            ) as creative_view
        from raw
        group by
            line_item_id, line_item_type, c_line_item_type, campaign_id, age,
            gender, date
    ),
    /* Change data for line_item_type = display and   c_line_item_type  not display and agrregate data back to lineitem level*/
    track4 as (
        select
            line_item_id,
            line_item_type,
            c_line_item_type,
            campaign_id,
            age,
            gender,
            to_date(date, 'YYYY-MM-DD') as date,
            sum(impression) as impression,
            sum(click) as click,
            sum(
                case
                    when
                        (lower(line_item_type)) = 'display'
                        and (lower(c_line_item_type))
                        not in ('display', 'retargeted_banner')
                    then impression
                    else complete
                end
            ) as complete,
            sum(
                case
                    when
                        (lower(line_item_type)) = 'display'
                        and (lower(c_line_item_type))
                        not in ('display', 'retargeted_banner')
                    then impression
                    else creative_view
                end
            ) as creative_view
        from track_triton
        group by
            line_item_id, line_item_type, c_line_item_type, campaign_id, age,
            gender, date
    ),
    merged1 as (
        select
            t4.line_item_id,
            t4.line_item_type,
            t4.c_line_item_type,
            t4.age,
            t4.gender,
            t4.impression,
            t4.click,
            t4.date,
            t4.complete,
            t4.creative_view,
            camp_detail.campaign_name,
            camp_detail.campaign_end_date
        from track4 t4
        left join camp_detail on t4.campaign_id = camp_detail.campaign_id
    ),

    
    merge_ctr as (
        select
            line_item_type,
            c_line_item_type,
            age,
            gender,
            cleaned_device_os,
            impression,
            click,
            complete,
            creative_view,
            campaign_name,
            case
                when lower(c_line_item_type) in ('display', 'retargeted_banner')
                then impression
                else creative_view
            end as ctr_impressions
        from merged2
        where date <= (campaign_end_date + 2)
    ),
    final as (
        select
            campaign_name as "Campaign Name",
            initcap(c_line_item_type) as c_line_item_type,
            cleaned_device_os,
            sum(impression) as "Delivered Impressions",
            cast(sum(creative_view) as varchar) as "Delivered companion Impressions",
            sum(click) as "Clicks"
        from merge_ctr
        group by "Campaign Name", c_line_item_type, cleaned_device_os
    ),
    final2 as (
        select "Campaign Name", sum("Delivered Impressions") as "TOTAL Impressions"
        from final
        group by "Campaign Name"
    ),
    total as (
        select
            final."Campaign Name",
            cleaned_device_os as "Device OS",
            sum("Delivered Impressions") as "Delivered Impressions",
            final2."TOTAL Impressions"
        from final
        left join final2 on final."Campaign Name" = final2."Campaign Name"
        group by final."Campaign Name", cleaned_device_os, final2."TOTAL Impressions"
        order by "Delivered Impressions" desc
    )
select
    "Device OS",
    concat(
        round(("Delivered Impressions" * 100) / nullif("TOTAL Impressions", 0), 2), '%'
    ) as "Split",
    "Delivered Impressions"::int as "Delivered Impressions"
from total
;
