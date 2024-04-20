with

    adswizz_podcast_bids_jan11 as (

        select

            -- bid -- 
            `bidreq.cur_0_` as bid_cur_0,
            `bidreq.cur_1_` as bid_cur_1,
            `bidreq.cur_2_` as bid_cur_2,
            `bidreq.cur_3_` as bid_cur_3,
            `bidreq.at` as bid_at,
            `bidreq.id` as bid_id,
            `bidid` as bidid,

            -- bid user --
            `bidreq.user.id` as bid_user_id,

            -- device -- 
            `bidreq.device.os` as bid_device_os,
            `bidreq.device.devicetype` as bid_device_type,
            `bidreq.device.ipv6` as bid_device_ipv6,
            `bidreq.device.ip` as bid_device_ip,
            `bidreq.device.lmt` as bid_device_lmt,
            `bidreq.device.dnt` as bid_device_dnt,

            -- device geo --
            `bidreq.device.geo.utcoffset` as bid_device_geo_utcoffset,
            `bidreq.device.geo.zip` as bid_device_geo_zip,
            `bidreq.device.geo.city` as bid_device_geo_city,
            `bidreq.device.geo.region` as bid_device_geo_region,
            `bidreq.device.geo.country` as bid_device_geo_country,
            `bidreq.device.ua` as bid_device_ua,
            `bidreq.device.ifa` as device_ifa,
            `bidreq.device.language` as device_language,
            `bidreq.device.model` as device_model,

            -- site content -- 
            `bidreq.site.content.genre` as site_content_genre,
            `bidreq.site.content.language` as site_content_language,
            `bidreq.site.content.cat_0_` as site_content_cat,
            `bidreq.site.content.url` as site_content_url,
            `bidreq.site.content.series` as site_content_series,
            `bidreq.site.content.title` as site_content_title,
            `bidreq.site.content.id` as site_content_id,
            `bidreq.site.content.episode` as site_content_episode,
            `bidreq.site.content.season` as site_content_season,

            -- site publisher -- 
            `bidreq.site.publisher.domain` as site_publisher_domain,
            `bidreq.site.publisher.cat_0_` as site_publisher_cat,
            `bidreq.site.publisher.name` as site_publisher_name,
            `bidreq.site.publisher.id` as site_publisher_id,

            -- site --
            `bidreq.site.page` as site_page,
            `bidreq.site.cat_0_` as site_cat,
            `bidreq.site.domain` as site_domain,
            `bidreq.site.name` as site_name,
            `bidreq.site.id` as site_id,

            -- imp pmp --
            `bidreq.imp_0_.pmp.deals_0_.at` as imp_pmp_deals_0_at,
            `bidreq.imp_0_.pmp.deals_0_.bidfloorcur` as imp_pmp_deals_0_bidfloorcur,
            `bidreq.imp_0_.pmp.deals_0_.bidfloor` as imp_pmp_deals_0_bidfloor,
            `bidreq.imp_0_.pmp.deals_0_.id` as imp_pmp_deals_0_id,
            `bidreq.imp_0_.pmp.private_auction` as imp_pmp_private_auction,

            -- imp --
            `bidreq.imp_0_.secure` as imp_secure,
            `bidreq.imp_0_.bidfloorcur` as imp_bidfloorcur,
            `bidreq.imp_0_.bidfloor` as imp_bidfloor,
            `bidreq.imp_0_.id` as imp_id,

            -- imp audio --
            `bidreq.imp_0_.audio.stitched` as imp_audio_stiched,
            `bidreq.imp_0_.audio.feed` as imp_audio_feed,
            `bidreq.imp_0_.audio.maxseq` as imp_audio_maxseq,
            `bidreq.imp_0_.audio.startdelay` as imp_audio_startdelay,
            `bidreq.imp_0_.audio.maxduration` as imp_audio_maxduration,
            `bidreq.imp_0_.audio.minduration` as imp_audio_minduration,
            `bidreq.imp_0_.audio.delivery_0_` as imp_audio_delivery_0,

            -- imp audio companion --
            `bidreq.imp_0_.audio.companiontype_0_` as imp_audio_companion_type_0,
            `bidreq.imp_0_.audio.companiontype_1_` as imp_audio_companion_type_1,
            `bidreq.imp_0_.audio.companiontype_2_` as imp_audio_companion_type_2,
            `bidreq.imp_0_.audio.companionad_0_.h` as imp_audio_companion_ad_0_h,
            `bidreq.imp_0_.audio.companionad_1_.h` as imp_audio_companion_ad_1_h,
            `bidreq.imp_0_.audio.companionad_2_.h` as imp_audio_companion_ad_2_h,
            `bidreq.imp_0_.audio.companionad_3_.h` as imp_audio_companion_ad_3_h,
            `bidreq.imp_0_.audio.companionad_0_.w` as imp_audio_companion_ad_0_w,
            `bidreq.imp_0_.audio.companionad_1_.w` as imp_audio_companion_ad_1_w,
            `bidreq.imp_0_.audio.companionad_2_.w` as imp_audio_companion_ad_2_w,
            `bidreq.imp_0_.audio.companionad_3_.w` as imp_audio_companion_ad_3_w,

            -- app -- 
            `bidreq.app.domain` as app_domain,
            `bidreq.app.bundle` as app_bundle,
            `bidreq.app.name` as app_app_name,
            `bidreq.app.id` as app_app_id,
            `bidreq.app.cat_0_` as app_cat,
            `bidreq.app.storeurl` as app_storeurl,

            -- app publisher --
            `bidreq.app.publisher.domain` as app_publisher_domain,
            `bidreq.app.publisher.name` as app_publisher_name,
            `bidreq.app.publisher.id` as app_publisher_id,
            `bidreq.app.publisher.cat_0_` as app_publisher_cat,

            -- app content --
            `bidreq.app.content.language` as app_content_language,
            `bidreq.app.content.genre` as app_content_genre,
            `bidreq.app.content.cat_0_` as app_content_cat,
            `bidreq.app.content.url` as app_content_url,
            `bidreq.app.content.season` as app_content_season,
            `bidreq.app.content.series` as app_content_series,
            `bidreq.app.content.title` as app_content_title,
            `bidreq.app.content.episode` as app_content_episode,
            `bidreq.app.content.id` as app_content_id,

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

        from paytunes_data.adswizz_podcast_bids_jan11

    )

select *
from adswizz_podcast_bids_jan11
