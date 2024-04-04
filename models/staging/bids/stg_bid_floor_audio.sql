with

    audio_bids as (

        select

            `_id.ssp` as ssp,
            `_id.ad_type` as ad_type,
            `_id.feed` as feed,
            `_id.dealcode[0][0]` as dealcode,
            `_id.app_id` as ssp_app_id,
            `_id.app_name` as ssp_app_name,
            `_id.bundle` as bundle,
            `_id.publisher_id` as publisher_id,
            `_id.floor_price` as fp,
            `_id.date` as date,
            bid_count

        from paytunes_data.bid_floor_audio

    )
{% do log("fdgfsdv *******************************************", info=true) %}
{% do log(node, info=true) %}
    {% do log("fdgfsdv *******************************************", info=true) %}
select * from audio_bids
