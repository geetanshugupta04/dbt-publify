with

    camp_detail as (
        select *
        from {{ ref("stg_campaigns") }}
        where campaign_id = 'ebpCArhq7MgQK25u6fzdg8'
    ),

    line_detail as (

        select *
        from {{ ref("stg_lineitems") }}
        where campaign_id in (select campaign_id from camp_detail)
    ),

    track_geo_hourly as (
        select *
        from {{ ref("stg_track_geo_campaigns") }}
        where line_item_id in (select line_item_id from line_detail)
    ),

    pub_detail as (select * from {{ ref("stg_publishers") }}),

    geo_map as (select * from {{ ref("stg_pincode_metadata") }}),

    /* Take out impression data*/
    track1 as (
        select
            line_item_id,
            ssp,
            coalesce(pincode, 'NA') as pincode,
            app_id,
            date,
            sum(impression) as impression,
            sum(creative_view) as creative_view,
            sum(click) as click,
            sum(complete) as complete
        from track_geo_hourly
        where line_item_id in (select line_item_id from line_detail)
        group by line_item_id, pincode, app_id, date, ssp

    ),

    /* offline pincode adjustments start*/
    track_pincode as (
        select
            line_item_id,
            pincode,
            app_id,
            date,
            sum(impression) as impression,
            sum(click) as click,
            sum(
                case when ssp = 'offline' then impression else complete end
            ) as complete,
            sum(
                case when ssp = 'offline' then impression else creative_view end
            ) as creative_view

        from track1

        group by line_item_id, pincode, app_id, date
    ),

    track_pincode_lineitems as (
        select l.campaign_id, l.c_line_item_type, t.*

        from track_pincode as t
        left join line_detail as l on t.line_item_id = l.line_item_id

    ),

    sum_by_pin as (
        select
            campaign_id,
            c_line_item_type,
            sum(impression) as sum_imp_pin,
            sum(click) as sum_click_pin,
            sum(creative_view) as sum_creative_view_pin,
            sum(complete) as sum_complete_pin
        from track_pincode_lineitems
        where pincode <> 'NA'
        group by campaign_id, c_line_item_type
    ),

    sum_na as (
        select
            campaign_id,
            c_line_item_type,

            sum(impression) as sum_imp_na,
            sum(click) as sum_click_na,
            sum(creative_view) as sum_creative_view_na,
            sum(complete) as sum_complete_na
        from track_pincode_lineitems
        where pincode = 'NA'
        group by campaign_id, c_line_item_type
    ),

    sum_pincode_merged as (
        select
            imp.*,
            sum_pin.sum_imp_pin,
            sum_pin.sum_click_pin,
            sum_pin.sum_creative_view_pin,
            sum_pin.sum_complete_pin,
            case
                when sum_na.sum_imp_na is null then 0 else sum_na.sum_imp_na
            end as sum_imp_na,
            case
                when sum_na.sum_click_na is null then 0 else sum_na.sum_click_na
            end as sum_click_na,
            case
                when sum_na.sum_complete_na is null then 0 else sum_na.sum_complete_na
            end as sum_complete_na,
            case
                when sum_na.sum_creative_view_na is null
                then 0
                else sum_na.sum_creative_view_na
            end as sum_creative_view_na,

            (imp.impression / sum_pin.sum_imp_pin) as imp_percent,
            (imp.click / sum_pin.sum_click_pin) as click_percent,
            (
                imp.creative_view / sum_pin.sum_creative_view_pin
            ) as creative_view_percent,
            (imp.complete / sum_pin.sum_complete_pin) as complete_percent
        from (select * from track_pincode_lineitems where pincode <> 'NA') as imp
        left join
            sum_by_pin as sum_pin
            on imp.c_line_item_type = sum_pin.c_line_item_type
            and imp.campaign_id = sum_pin.campaign_id
        left join
            sum_na
            on imp.c_line_item_type = sum_na.c_line_item_type
            and imp.campaign_id = sum_na.campaign_id

    ),

    track1_pincode_adjusted as (
        select
            campaign_id,
            c_line_item_type,
            line_item_id,
            app_id,
            date,
            pincode,
            (impression + (sum_imp_na * imp_percent)) as impression,
            (click + (sum_click_na * click_percent)) as click,
            creative_view_percent,
            creative_view as creative_view_old,
            (
                creative_view + (sum_creative_view_na * creative_view_percent)
            ) as creative_view,
            complete_percent,
            complete as complete_old,
            (complete + (sum_complete_na * complete_percent)) as complete

        from sum_pincode_merged
    ),

    /* offline pincode adjustments ends*/
    /* Take out Publisher data*/
    -- pub_detail as (
    -- select id as app_id, name as "Publisher Group", ssp as ssp
    -- from metadata_publisher
    -- ),
    merged as (
        select
            t1.campaign_id,
            t1.line_item_id,
            t1.pincode,
            t1.app_id,
            t1.date,
            t1.impression,
            t1.creative_view,
            t1.click,
            t1.complete,
            pub_detail.publisher_group,
            pub_detail.ssp
        from track1_pincode_adjusted t1
        left join pub_detail on t1.app_id = pub_detail.app_id
    ),

    track2 as (
        select
            campaign_id,
            line_item_id,
            pincode,
            app_id,
            date,
            publisher_group,
            ssp,
            sum(impression) as impression,
            sum(click) as click,
            sum(complete) as complete,
            sum(creative_view) as creative_view

        from merged
        group by campaign_id, line_item_id, pincode, app_id, date, publisher_group, ssp
    ),

    /* Change creative view data for spotify and saavn publisher*/
    track3 as (
        select
            line_item_id,
            app_id,
            pincode,
            publisher_group,
            date,
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
        group by line_item_id, app_id, pincode, publisher_group, ssp, date
    ),
    /* Merge Line item metadata to line item- pub level data*/
    raw as (
        select
            t.line_item_id,
            t.app_id,
            t.pincode,
            t.publisher_group,
            t.date,
            t.ssp,
            t.impression,
            t.click,
            t.complete,
            t.creative_view,
            line_detail.campaign_id,
            line_detail.line_item_name,
            line_detail.line_item_type,
            line_detail.c_line_item_type,
            line_detail.c_line_item_name
        from track3 t
        left join line_detail on t.line_item_id = line_detail.line_item_id
    ),
    /* Change data for Triton Audio  using global parameter  for 90% */
    track_triton as (
        select
            line_item_id,
            line_item_type,
            c_line_item_name,
            c_line_item_type,
            campaign_id,
            publisher_group,
            ssp,
            pincode,
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
            c_line_item_name,
            c_line_item_type,
            campaign_id,
            publisher_group,
            ssp,
            pincode,
            date
    ),
    /* Change data for line_item_type = display and   c_line_item_type  not display and agrregate data back to lineitem level*/
    track4 as (
        select
            line_item_id,
            line_item_type,
            c_line_item_name,
            c_line_item_type,
            campaign_id,
            publisher_group,
            ssp,
            pincode,
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
            c_line_item_name,
            c_line_item_type,
            campaign_id,
            publisher_group,
            ssp,
            pincode,
            date
    ),
    
    merged1 as (
        select
            r.line_item_id,
            r.line_item_type,
            r.c_line_item_name,
            r.c_line_item_type,
            r.campaign_id,
            r.publisher_group,
            r.ssp,
            r.pincode,
            r.date,
            r.impression,
            r.click,
            r.complete,
            r.creative_view,
            camp_detail.campaign_name,
            camp_detail.campaign_end_date,
            camp_detail.show_avg_cpm
        from track4 r
        left join camp_detail on r.campaign_id = camp_detail.campaign_id
    ),
    merged2 as (
        select
            m.line_item_id,
            m.line_item_type,
            m.c_line_item_name,
            m.c_line_item_type,
            m.campaign_id,
            m.date,
            m.publisher_group,
            m.ssp,
            m.pincode,
            m.impression,
            m.click,
            m.complete,
            m.creative_view,
            m.campaign_name,
            m.campaign_end_date,
            m.show_avg_cpm,
            geo_map.urban_or_rural,
            geo_map.city,
            geo_map.grand_city,
            geo_map.state
        from merged1 m
        left join geo_map on m.pincode = geo_map.pincode
    ),
    merge_ctr as (
        select
            line_item_type,
            c_line_item_name,
            c_line_item_type,
            publisher_group,
            ssp,
            date,
            impression,
            click,
            complete,
            creative_view,
            campaign_name,
            campaign_end_date,
            city,
            show_avg_cpm,
            grand_city,
            case
                when lower(c_line_item_type) in ('display', 'retargeted_banner')
                then impression
                else creative_view
            end as ctr_impressions
        from merged2
        where show_avg_cpm = true
    ),
    
    merged3 as (
        select
            c_line_item_name as c_line_item_name,
            initcap(c_line_item_type) as c_line_item_type,
            case
                when grand_city = '' then city else grand_city
            end as grand_city_or_city,
            sum(impression) as impression,
            sum(creative_view) as creative_view,
            sum(click) as click,
            sum(complete) as complete,
            sum(ctr_impressions) as ctr_impressions,
            row_number() over (
                partition by c_line_item_name order by sum(impression) desc
            ) as rnm
        from merge_ctr
        -- where date <= (campaign_end_date + 2)
        group by c_line_item_name, c_line_item_type, grand_city_or_city
    ),

    all_cities as (
        select
            c_line_item_name,
            c_line_item_type,
            grand_city_or_city,
            impression,
            creative_view,
            click,
            complete,
            ctr_impressions,
            rnm
        from merged3
        where grand_city_or_city != ''
    ),
    final_cities as (
        select
            c_line_item_name,
            c_line_item_type,
            grand_city_or_city,
            impression,
            cast(creative_view as varchar(10)),
            click,
            complete,
            ctr_impressions,
            rnm
        from all_cities
        where rnm <= 10

        union all

        select
            c_line_item_name,
            c_line_item_type,
            'Others' as grand_city_or_city,
            sum(impression) as impression,
            cast(sum(creative_view) as varchar(10)) as creative_view,
            sum(click) as click,
            sum(complete) as complete,
            sum(ctr_impressions) as ctr_impressions,
            11 as rnm
        from all_cities
        where rnm > 10
        group by c_line_item_name, c_line_item_type
    ),


    final as (
        select
            c_line_item_name,
            c_line_item_type,
            grand_city_or_city,
            impression,
            case
                when lower(c_line_item_type) in ('display', 'retargeted_banner')
                then 'NA'
                else creative_view
            end as creative_view,
            cast(click as int) as clicks,
            case
                when lower(c_line_item_type) in ('display', 'retargeted_banner')
                then 'NA'
                else
                    coalesce(
                        nullif(
                            concat(
                                round((complete * 100 / nullif(impression, 0)), 2), '%'
                            ),
                            '%'
                        ),
                        '-'
                    )
            end as vtr_ltr,
            nullif(
                concat(round((click * 100 / nullif(ctr_impressions, 0)), 2), '%'), '%'
            ) as ctr
        from final_cities
        order by c_line_item_name, c_line_item_type, rnm

    )

select *
from final
