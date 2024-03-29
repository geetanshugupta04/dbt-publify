with

    track as (
        select

            type,
            -- id,
            -- appid,
            -- campaignid as line_item_id,
            zip as pincode,
            -- ip as ip,
            rtbtype as ad_type,
            -- phonemake as device_make,
            -- phonemodel as device_model,
            platformtype as device_os,
            osversion as device_os_version,
            -- ifa,
            -- region,
            -- language,
            platform_type,
            -- bundle,
            -- bundlename as bundle_name,
            -- citylanguage as city_language,
            -- apppubid as app_publisher_id,
            ssp,
            -- category,
            -- feed,
            -- deal_id,
            -- report_name,
            -- yob,
            -- gender,
            -- cont_lang as content_language,
            -- campaign as campaign_id,
            -- country,
            -- device_type,
            -- platform_type as platform_type_2,
            -- c_line_item_type,
            -- dealcode,
            -- enable_retargeting,
            -- user_id,
            -- ssp_uid,
            -- lat,
            -- lon,
            -- episode_title,
            -- episode_series,
            domain,
            app_name,
            p_bundle,
            -- date,
            app_id,
            -- floor_price,
            -- floor_currency,
            -- bid_price,
            -- cont_cat as content_category,
            -- date_trunc('minute', convert_timezone('IST', savedon)) as saved_on,
            -- date_trunc('minute', convert_timezone('IST', receivedon)) as received_on,
            -- date_trunc('minute', convert_timezone('IST', createdon)) as created_on,
            -- convert_timezone('IST', savedon) as saved_on,
            -- convert_timezone('IST', receivedon) as received_on,
            convert_timezone('IST', createdon) as created_on

        from paytunes_data.track_oct9_15
    )

select *
from track
