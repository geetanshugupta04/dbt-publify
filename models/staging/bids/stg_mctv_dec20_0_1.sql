with

    bids_mctv as (

        select

            -- ---- device -------
            `bidreq.device.make` as bid_device_make,
            `bidreq.device.model` as bid_device_model,
            `bidreq.device.osv` as bid_device_osv,
            `bidreq.device.devicetype` as bid_device_type,
            `bidreq.device.ua` as bid_device_ua,
            `bidreq.device.language` as bid_device_language,
            `bidreq.device.ifa` as bid_device_ifa,
            `bidreq.device.os` as bid_device_os,
            `bidreq.device.ip` as bid_device_ip,
            `bidreq.device.dpidmd5` as bid_device_dpidmd5,
            -- dpi: device platform ID
            -- did: hardware device ID
            `bidreq.device.connectiontype` as bid_device_connection_type,  -- RTB doc 5.22 

            -- ---- geo -------
            `bidreq.device.geo.lat` as device_geo_lat,
            `bidreq.device.geo.region` as devie_geo_region,
            `bidreq.device.geo.lon` as device_geo_lon,
            `bidreq.device.geo.metro` as device_geo_is_metro,
            `bidreq.device.geo.country` as device_geo_country,
            `bidreq.device.geo.zip` as device_pincode,

            -- ---- imp --------
            `bidreq.imp_0_.instl` as imp_instl,  -- 1 = the ad is interstitial or full screen, 0 = not instl
            `bidreq.imp_0_.bidfloorcur` as imp_bid_floor_curr,
            `bidreq.imp_0_.bidfloor` as imp_bid_floor,
            `bidreq.imp_0_.tagid` as imp_tag_id,  -- This can be useful for optimization by the buyer.
            `bidreq.imp_0_.ext.ssai` as imp_ssai,

            -- ---- imp video ------
            `bidreq.imp_0_.video.minduration` as imp_video_min_duration,
            `bidreq.imp_0_.video.maxduration` as imp_video_max_duration,

            `bidreq.imp_0_.video.startdelay` as imp_video_startdelay,  -- RTB doc 5.12 pg 49
            `bidreq.imp_0_.video.w` as imp_video_w,
            `bidreq.imp_0_.video.h` as imp_video_h,
            `bidreq.imp_0_.video.placement` as imp_video_placement,  -- RTB doc 5.9 pg 47 
            `bidreq.imp_0_.video.linearity` as imp_video_linearity,
            /*
linearity: “In-stream” or “linear” video refers to pre-
roll, post-roll, or mid-roll video ads where the user is forced to watch ad in order to see the video
content. “Overlay” or “non-linear” refer to ads that are shown on top of the video content.
*/
            `bidreq.imp_0_.video.maxextended` as imp_video_maxextended,
            `bidreq.imp_0_.video.boxingallowed` as imp_video_boxingallowed,
            `bidreq.imp_0_.video.playbackmethod_0_` as imp_video_playbackmethod,  -- there may be an array in raw data, use only the first
            `bidreq.imp_0_.video.pos` as imp_video_position,  -- rtb doc 5.4
            `bidreq.imp_0_.video.skip` as imp_video_skip,

            `bidreq.imp_0_.video.skipafter` as imp_video_skipafter,
            `bidreq.imp_0_.video.skipmin` as imp_video_skipmin,
            `bidreq.imp_0_.video.delivery_0_` as imp_video_delivery,

            -- ---- app -------
            `bidreq.app.bundle` as app_bundle,
            `bidreq.app.id` as app_app_id,
            `bidreq.app.name` as app_app_name,
            `bidreq.app.cat_0_` as app_category_0,
            `bidreq.app.cat_1_` as app_category_2,
            `bidreq.app.domain` as app_domain,
            `bidreq.app.paid` as app_paid,  -- app is free or paid verion
            `bidreq.app.ver` as app_version,

            -- ---- app publisher -------
            `bidreq.app.publisher.id` as app_publisher_id,
            `bidreq.app.publisher.name` as app_publisher_name,
            `bidreq.app.publisher.domain` as app_publisher_domain,
            `bidreq.app.storeurl` as app_store_url,  -- app store url

            -- --- app content -----
            `bidreq.app.content.livestream` as app_content_livestream,  -- content live or not (1,0)
            `bidreq.app.content.ext.network` as app_content_ext_network,

            `bidreq.app.content.prodq` as app_content_production_quality,  -- rtb doc 5.13
            `bidreq.app.content.title` as app_content_title,
            `bidreq.app.content.genre` as app_content_genre,
            `bidreq.app.content.series` as app_content_series,
            `bidreq.app.content.len` as app_content_len,
            `bidreq.app.content.ext.channel` as app_content_ext_channel,
            `bidreq.app.content.cat_0_` as app_content_cat_0,
            `bidreq.app.content.cat_1_` as app_content_cat_1,
            `bidreq.app.content.cat_2_` as app_content_cat_2,
            `bidreq.app.content.cat_3_` as app_content_cat_3,
            `bidreq.app.content.cat_4_` as app_content_cat_4,
            `bidreq.app.content.cat_5_` as app_content_cat_5,
            `bidreq.app.content.producer.name` as app_content_producer_name,
            `bidreq.app.content.contentrating` as app_content_rating,
            `bidreq.app.content.episode` as app_content_episode,
            `bidreq.app.content.season` as app_content_season,
            `bidreq.app.content.url` as app_content_url,

            -- ---- site --------
            `bidreq.site.page` as site_page,  -- page url
            `bidreq.site.id` as site_id,
            `bidreq.site.name` as site_name,
            `bidreq.site.domain` as site_domain,
            `bidreq.site.ref` as site_ref,  -- URL that caused navigation to this site
            `bidreq.site.privacypolicy` as site_privacypolicy,  -- if the site has a privacy policy or not
            `bidreq.site.cat_0_` as site_cat_0,
            `bidreq.site.cat_1_` as site_cat_1,
            `bidreq.site.cat_2_` as site_cat_2,
            `bidreq.site.cat_3_` as site_cat_3,
            `bidreq.site.cat_4_` as site_cat_4,
            `bidreq.site.cat_5_` as site_cat_5,
            `bidreq.site.cat_6_` as site_cat_6,
            `bidreq.site.cat_7_` as site_cat_7,
            `bidreq.site.cat_8_` as site_cat_8,
            `bidreq.site.cat_9_` as site_cat_9,
            `bidreq.site.cat_10_` as site_cat_10,
            `bidreq.site.cat_11_` as site_cat_11,
            `bidreq.site.cat_12_` as site_cat_12,
            `bidreq.site.cat_13_` as site_cat_13,
            `bidreq.site.cat_14_` as site_cat_14,
            `bidreq.site.cat_15_` as site_cat_15,

            -- --- site publisher ------
            `bidreq.site.publisher.id` as site_publisher_id,
            `bidreq.site.publisher.name` as site_publisher_name,
            `bidreq.site.publisher.domain` as site_publisher_domain,

            -- --- site content ----
            `bidreq.site.content.livestream` as site_content_livestream,
            `bidreq.site.content.genre` as site_content_genre,
            `bidreq.site.content.prodq` as site_content_production_quality,
            `bidreq.site.content.contentrating` as site_content_content_rating,  -- rtb doc example: MPAA (PG, A, etc.)
            `bidreq.site.content.len` as site_content_len,
            `bidreq.site.content.url` as site_content_url,
            `bidreq.site.content.title` as site_content_title,
            `bidreq.site.content.ext.network` as site_content_ext_network,
            `bidreq.site.content.ext.channel` as site_content_ext_channel,
            `bidreq.site.content.series` as site_content_series,
            `bidreq.site.content.cat_0_` as site_content_cat,

            -- --- imp pmp ----- private marketplace deals between buyers and sellers
            `bidreq.imp_0_.pmp.private_auction` as imp_pmp_private_auction,
            `bidreq.imp_0_.pmp.deals_0_.id` as imp_pmp_deals_id,
            `bidreq.imp_0_.pmp.deals_0_.bidfloorcur` as imp_pmp_deals_bidfloorcur,
            `bidreq.imp_0_.pmp.deals_0_.bidfloor` as imp_pmp_deals_bidfloor,
            `bidreq.imp_0_.pmp.deals_0_.at` as imp_pmp_deals_at,

            `bidreq.at` as auction_at,
            `bidreq.user.ext.consent` as user_ext_consent,
            `bidreq.regs.ext.gdpr` as regs_ext_gdpr,

            bidid,
            rtbreqid,
            ssp,
            country,
            ad_type,
            device_os,
            device_type,
            zip,
            make,
            model,
            platform_type,
            device_lang,
            gender,
            yob,
            category,
            publisher_id,
            publisher_name,
            bundle,
            app_name,
            app_id,
            domain,
            page,
            feed,
            cont_lang,
            episode_title,
            episode_series,
            floor_currency,
            line_item_id,
            appid,
            dealcode,
            createdon,
            bid_price,
            floor_price,
            deal_id,
            bidstatus

        -- bcat blocked categories, consult if required
        -- bidreq.imp_0_.ext.ssai,
        from paytunes_data.mctv_dec20_0_1
    )

select *
from bids_mctv
