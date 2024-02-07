with

    triton_podcast_bids as (

        select

            `bidreq.id` as bid_id,
            -- `bidreq.badv[0]`, -- block list of advertisers
            -- `bidreq.bcat[0]`, -- blocked advertiser categories
            -- `bidreq.cur[0]`,
            -- --- user -----
            `bidreq.user.gender` as user_gender,
            `bidreq.user.yob` as user_yob,
            `bidreq.user.id` as user_id,

            -- --- device -----
            `bidreq.device.ext.ifa_type` as device_ifa_type,
            `bidreq.device.ifa` as device_ifa,
            `bidreq.device.language` as device_language,
            `bidreq.device.model` as device_model,
            `bidreq.device.os` as device_device_os,
            `bidreq.device.devicetype` as device_device_type,
            `bidreq.device.geo.zip` as device_pincode,
            `bidreq.device.geo.country` as device_country,
            `bidreq.device.geo.ipservice` as device_ipservice,
            `bidreq.device.geo.type` as device_geo_type,  -- Source of location data; recommended when passing lat/lon.
            `bidreq.device.geo.lon` as device_lon,
            `bidreq.device.geo.lat` as device_lat,
            `bidreq.device.ip` as device_ip,
            `bidreq.device.ua` as device_ua,
            `bidreq.device.dnt` as device_donottrack,  -- do not track in the browser, 0 = tracking is unrestricted, 1 = do not track

            -- --- site -----
            `bidreq.site.page` as site_page,  -- URL of the page where the impression will be shown
            `bidreq.site.publisher.name` as site_publisher_name,
            `bidreq.site.publisher.id` as site_publisher_id,
            `bidreq.site.cat[0]` as site_cat,  -- array of IAB content categories of the site

            `bidreq.site.domain` as site_domain,
            `bidreq.site.name` as site_name,
            `bidreq.site.id` as site_id,  -- exchange specific site ID

            -- --- site content -----
            `bidreq.site.content.language` as site_content_language,
            `bidreq.site.content.context` as site_content_context,  -- game, video, text, etc.
            `bidreq.site.content.prodq` as site_content_prodq,  -- production quality
            `bidreq.site.content.len` as site_content_len,
            `bidreq.site.content.genre` as site_content_genre,
            `bidreq.site.content.id` as site_content_id,
            `bidreq.site.content.url` as site_content_url,
            `bidreq.site.content.title` as site_content_title,
            `bidreq.site.content.episode` as site_content_episode,
            `bidreq.site.content.season` as site_content_season,
            `bidreq.site.content.series` as site_content_series,

            -- --- app -----
            `bidreq.app.storeurl` as app_storeurl,
            `bidreq.app.bundle` as app_bundle,
            `bidreq.app.publisher.name` as app_publisher_name,
            `bidreq.app.publisher.id` as app_publisher_id,
            `bidreq.app.cat[0]` as app_cat,
            `bidreq.app.domain` as app_domain,
            `bidreq.app.name` as app_app_name,
            `bidreq.app.id` as app_app_id,

            -- --- app content -----
            `bidreq.app.content.language` as app_content_language,
            `bidreq.app.content.context` as app_content_context,
            `bidreq.app.content.prodq` as app_content_prodq,
            `bidreq.app.content.len` as app_content_len,
            `bidreq.app.content.genre` as app_content_genre,
            `bidreq.app.content.id` as app_content_id,
            `bidreq.app.content.url` as app_content_url,
            `bidreq.app.content.title` as app_content_title,
            `bidreq.app.content.episode` as app_content_episode,
            `bidreq.app.content.season` as app_content_season,
            `bidreq.app.content.series` as app_content_series,

            -- --- imp -----
            `bidreq.imp[0].rwdd` as rwdd,
            `bidreq.imp[0].exp` as exp,  -- number of seconds that may elapse between the auction and actual impression
            `bidreq.imp[0].id` as id,

            `bidreq.imp[0].bidfloorcur` as bidfloorcur,
            `bidreq.imp[0].bidfloor` as bidfloor,

            -- ----- imp pmp -----
            `bidreq.imp[0].pmp.deals[0].ext.deal_type` as imp_pmp_deals_0_ext_deal_type,
            `bidreq.imp[0].pmp.deals[1].ext.deal_type` as imp_pmp_deals_1_ext_deal_type,
            `bidreq.imp[0].pmp.deals[0].at` as imp_pmp_deals_0_at,
            `bidreq.imp[0].pmp.deals[1].at` as imp_pmp_deals_1_at,
            `bidreq.imp[0].pmp.deals[0].bidfloorcur` as imp_pmp_deals_0_bidfloorcur,
            `bidreq.imp[0].pmp.deals[1].bidfloorcur` as imp_pmp_deals_1_bidfloorcur,
            `bidreq.imp[0].pmp.deals[0].bidfloor` as imp_pmp_deals_0_bidfloor,
            `bidreq.imp[0].pmp.deals[1].bidfloor` as imp_pmp_deals_1_bidfloor,
            `bidreq.imp[0].pmp.deals[0].id` as imp_pmp_deals_0_id,
            `bidreq.imp[0].pmp.deals[1].id` as imp_pmp_deals_1_id,
            `bidreq.imp[0].pmp.private_auction` as imp_pmp_private_auction,

            -- --- imp audio -----
            `bidreq.imp[0].audio.maxseq` as maxseq,  -- the max no. of ads that can be played in an ad pod
            `bidreq.imp[0].audio.delivery[0]` as delivery,  -- delivery options like streaming, progressive, download
            `bidreq.imp[0].audio.maxbitrate` as maxbitrate,  -- max speed of the users internet?
            `bidreq.imp[0].audio.minbitrate` as minbitrate,  -- min speed of the users internet?
            `bidreq.imp[0].audio.maxextended` as maxextended,
            `bidreq.imp[0].audio.startdelay` as startdelay,  -- start delay in seconds for pre-roll, mid-roll, or post-roll ad placements

            `bidreq.imp[0].audio.maxduration` as maxduration,
            `bidreq.imp[0].audio.minduration` as minduration,

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

        from paytunes_data.triton_podcast_jan5_20_22

    )

select *
from triton_podcast_bids
limit 100