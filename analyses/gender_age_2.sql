/* Campaign X Gender Summary */
/* Take out campaign data and campaign_id level*/
with
    camp_detail as (
        select
            id as campaign_id,
            name as campaign_name,
            min(start_date) as campaign_start_date,
            max(end_date) as campaign_end_date
        from campaign_campaign
        where id = '9BeKeQKcfQCD2gmCBvvFvM'
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
        from track_pub_hourly
        where line_item_id in (select line_item_id from line_detail)
        group by line_item_id, app_id, dealcode, date
    ),

    deals as (
        select line_item_type, deal_type, deal_id, age, gender
        from metadata_dealid
        where is_active is true and deal_id in (select dealcode from track)
        group by 1, 2, 3, 4, 5

    ),

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
            deal_id,
            age,
            gender,
            sum(impression) as impression,
            sum(creative_view) as creative_view,
            sum(click) as click,
            sum(complete) as complete

        from track_deal_merge
        group by 1, 2, 3, 4, 5, 6, 7

    ),

    /* Take out Publisher data*/
    pub_detail as (
        select id as app_id, name as publisher_group, ssp as ssp from metadata_publisher
    ),
    track_deal_pub_merge as (
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
        from track_deal_pub_merge
        group by line_item_id, age, gender, publisher_group, ssp, date
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
        group by line_item_id, age, gender, publisher_group, ssp, date
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
            line_item_id,
            line_item_type,
            c_line_item_type,
            campaign_id,
            age,
            gender,
            date
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
            line_item_id,
            line_item_type,
            c_line_item_type,
            campaign_id,
            age,
            gender,
            date
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
            impression,
            click,
            complete,
            creative_view,
            campaign_name,
            case
                when lower(c_line_item_type) in ('display', 'retargeted_banner')
                then impression
                else creative_view
            end as ctr_impression
        from merged1
        where date <= (campaign_end_date + 2)
    ),

    age_gender_grouped as (

        select
            campaign_name,
            -- gender, 
            age,
            sum(impression) as impression,
            sum(click) as click,
            sum(complete) as complete,
            sum(creative_view) as creative_view,
            sum(ctr_impression) as ctr_impression
        from merge_ctr
        group by 1, 2

    ),

    -- check_if_age_gender_exists as (
    -- select 
    -- case when exists (
    -- select 1 from age_gender_grouped where age in ('13-17', '35-44', '45+',
    -- '25-34', '18-24')
    -- ) then 'yes' else 'no'	end as exists_or_not		
    -- from age_gender_grouped
    -- ),
    -- check_if_age_gender_exists2 as (
    -- select 
    -- case when exists (
    -- select 1 from age_gender_grouped where age in ('13-17', '35-44', '45+',
    -- '25-34', '18-24')
    -- ) then 'yes' else 'no'	end as exists_or_not		
    -- from age_gender_grouped
    -- ),
    age_gender_partial as (
        select
            campaign_name,
            sum(impression) as partial_impression_sum,
            sum(click) as partial_click_sum,
            sum(complete) as partial_complete_sum,
            sum(creative_view) as partial_creative_view_sum,
            sum(ctr_impression) as partial_ctr_impression_sum
        from age_gender_grouped
        where age not in ('unspecified')
        group by 1

    ),

    age_gender_unspecified as (

        select
            campaign_name,
            -- age,
            sum(impression) as unspecified_impression_sum,
            sum(click) as unspecified_click_sum,
            sum(complete) as unspecified_complete_sum,
            sum(creative_view) as unspecified_creative_view_sum,
            sum(ctr_impression) as unspecified_ctr_impression_sum
        from age_gender_grouped
        where age = 'unspecified'
        group by 1
    ),

    partial_unspecified_merge as (
        select
            p.campaign_name,
            -- age,
            p.partial_impression_sum,
            p.partial_click_sum,
            p.partial_complete_sum,
            p.partial_creative_view_sum,
            p.partial_ctr_impression_sum,

            u.unspecified_impression_sum,
            u.unspecified_click_sum,
            u.unspecified_complete_sum,
            u.unspecified_creative_view_sum,
            u.unspecified_ctr_impression_sum
        from age_gender_partial as p
        left join age_gender_unspecified as u using (campaign_name)

    ),

    age_gender_grouped_merge as (
        select
            g.campaign_name,
            -- gender, 
            g.age,
            g.impression,
            g.click,
            g.complete,
            g.creative_view,
            g.ctr_impression,

            pu.partial_impression_sum,
            pu.partial_click_sum,
            pu.partial_complete_sum,
            pu.partial_creative_view_sum,
            pu.partial_ctr_impression_sum,

            pu.unspecified_impression_sum,
            pu.unspecified_click_sum,
            pu.unspecified_complete_sum,
            pu.unspecified_creative_view_sum,
            pu.unspecified_ctr_impression_sum
        from age_gender_grouped as g
        left join partial_unspecified_merge as pu using (campaign_name)

    ),

    final_metrics as (
        select

            campaign_name,
            age,

            impression,
            click,
            complete,
            creative_view,
            ctr_impression,

            partial_impression_sum,
            partial_click_sum,
            partial_complete_sum,
            partial_creative_view_sum,
            partial_ctr_impression_sum,

            unspecified_impression_sum,
            unspecified_click_sum,
            unspecified_complete_sum,
            unspecified_creative_view_sum,
            unspecified_ctr_impression_sum,

            round(
                (
                    ((impression / partial_impression_sum) * unspecified_impression_sum)
                    + impression
                )
            ) as delivered_impressions,
            round(
                (((click / partial_click_sum) * unspecified_click_sum) + click)
            ) as delivered_clicks,
            round(
                (
                    ((complete / partial_complete_sum) * unspecified_complete_sum)
                    + complete
                )
            ) as delivered_complete,
            round(
                (
                    (
                        (creative_view / partial_creative_view_sum)
                        * unspecified_creative_view_sum
                    )
                    + creative_view
                )
            ) as delivered_companion_impressions,
            round(
                (
                    (
                        (ctr_impression / partial_ctr_impression_sum)
                        * unspecified_ctr_impression_sum
                    )
                    + ctr_impression
                )
            ) as delivered_ctr_impressions

        from age_gender_grouped_merge
        where age not in ('unspecified')

    ),

    -- select * from calculations
    final as (
        select
            campaign_name,
            age,
            -- gender,
            sum(delivered_impressions) as delivered_impressions,
            sum(delivered_companion_impressions) as creative_view,
            sum(delivered_clicks) as click,
            sum(delivered_complete) as complete,
            sum(delivered_ctr_impressions) as ctr_impressions
        from final_metrics
        group by 1, 2

    ),

    final2 as (
        select
            campaign_name,
            sum(delivered_impressions) as total_delivered_impressions,
            sum(ctr_impressions) as total_ctr_impressions
        from final
        group by 1
    ),

    total as (
        select
            final.campaign_name,
            age,
            -- gender,
            delivered_impressions,
            creative_view,
            click,
            complete,
            ctr_impressions,
            nullif(
                concat(
                    round((complete * 100 / nullif(delivered_impressions, 0)), 2), '%'
                ),
                '%'
            ) as "VTR/LTR",
            nullif(
                concat(round(click * 100 / nullif(ctr_impressions, 0), 2), '%'), '%'
            ) as "CTR",
            total_delivered_impressions

        from final
        left join final2 on final.campaign_name = final2.campaign_name
        order by delivered_impressions desc
    ),

    total2 as (

        select
            campaign_name as "Campaign Name",
            age as "Age Group",
            -- case
            -- when lower(gender) = lower('female')
            -- then 'Female'
            -- when lower(gender) = lower('Male')
            -- then 'Male'
            -- else 'Both'
            -- end as "Gender",
            concat(
                round(
                    (delivered_impressions * 100)
                    / nullif(total_delivered_impressions, 0),
                    2
                ),
                '%'
            ) as "Split",
            delivered_impressions::int as "Delivered Impressions",
            creative_view::int as "Delivered Companion Impressions",
            click::int as "Clicks",
            complete::int as "Complete",
            coalesce("VTR/LTR", '-') as "VTR/LTR",
            coalesce("CTR", '-') as "CTR"

        from total

    ),

    wynk_impressions as (

        select
            date,
            ad_type,
            bundle_name,
            dealcode,
            round(price, 2) as bid_price,
            sum(impression) as impressions
        from track_pub_hourly
        where date between '2024-01-01' and '2024-01-31' and bundle_name = 'WynkMusic'
        group by 1, 2, 3, 4
        order by date, dealcode, bid_price, impressions

    )

select *
from total2
