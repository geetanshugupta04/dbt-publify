{% set triton_pubs = [
    "zenoads",
    "zenofon",
    "odeeo",
    "audiomob",
    "pocketfm",
    "adtonos",
    "audioboom",
    "esound",
    "gadsme",
    "instreamatic",
    "libsyn",
    "podbean",
    "spreaker",
    "trinity",
    "blogtalkradio",
    "all_radio",
    "triton_podcast",
    "videobyte",
    "omny",
    "srgstr",
] %}

{% set adswizz_pubs = [
    "libsyn",
    "gadsme",
    "airwave",
    "acast",
    "atunwa",
    "audioboom",
    "audiomack",
    "audiomob",
    "bigradio",
    "blogtalkradio",
    "idobiradio",
    "lautfm",
    "openstream",
    "puradsifm",
    "pureplayradio",
    "radiocity",
    "redcircle",
    "spreaker",
    "trinity",
    "tunein",
    "zenomedia",
] %}


with
    ssp_apps_bids as (select * from {{ ref("int_ssp_app_merge") }}),

    cleaned_pubs_triton as (

        select
            ssp,
            ssp_app_name,
            case
                {% for pub in triton_pubs %}
                    when ssp_app_name ilike '%{{pub}}%' then '{{pub}}'
                {% endfor %}
                else lower(ssp_app_name)
            end as publisher_cleaned,
            sum(bid_counts) as bids
        from ssp_apps_bids
        where ssp = 'Triton'
        group by 1, 2
        having bids > 1000
        order by 2
    ),

    cleaned_pubs_adswizz as (

        select
            ssp,
            ssp_app_name,
            case
                {% for pub in adswizz_pubs %}
                    when ssp_app_name ilike '%{{pub}}%' then '{{pub}}'
                {% endfor %}
                else lower(ssp_app_name)
            end as publisher_cleaned,
            sum(bid_counts) as bids
        from ssp_apps_bids
        where ssp = 'Adswizz'
        group by 1, 2
        having bids > 100
        order by 1

    ),

    -- cleaned_pubs_rubicon as (
    -- select
    -- publisher_id,
    -- ssp_app_name,
    -- publify_app_name,
    -- avg_floor_price,
    -- case
    -- {% for pub in triton_pubs %}
    -- when ssp_app_name ilike '%{{pub}}%' then '{{pub}}'
    -- {% endfor %}
    -- else ssp_app_name
    -- end as publisher_cleaned,
    -- sum(bid_counts) as bids
    -- from ssp_apps_bids
    -- where
    -- ssp = 'Rubicon'
    -- and ad_type in ('audio', 'podcast')
    -- and publify_app_name not in ('wynk')
    -- group by 1, 2, 3, 4
    -- having bids > 10
    -- order by 1
    -- )
    cleaned_pubs_union as (

        select ssp, ssp_app_name, publisher_cleaned
        from cleaned_pubs_triton
        union all
        select ssp, ssp_app_name, publisher_cleaned
        from cleaned_pubs_adswizz
    ),

    joined as (

        select bids.*, pubs.publisher_cleaned

        from ssp_apps_bids as bids
        left join
            cleaned_pubs_union as pubs
            on bids.ssp = pubs.ssp
            and bids.ssp_app_name = pubs.ssp_app_name

    ),

    final as (

        select

            ssp,
            ad_type,
            platform_type,
            ssp_app_id,
            ssp_app_name,
            publisher_id,
            bundle,
            domain,
            avg_floor_price,
            bid_counts,
            publify_app_name,
            ssp_publisher_id,
            ssp_publisher_name,
            case
                when publify_ssp_publisher_name is null
                then publisher_cleaned
                else lower(publify_ssp_publisher_name)
            end as publify_ssp_publisher_name,
            publify_ssp_publisher_master_id,
            publisher_cleaned

        from joined

    )

select *
from
    final
    -- order by 3
    /*


zenoads
ts_zenofon
odeeo
audiomob
pocketfm


adtonos
audioboom
esound
gadsme
instreamatic
libsyn
podbean
spreaker
trinity
blogtalkradio
all_radio
triton_podcast
videobyte
web1

adswizz

libsyn
gadsme
airwave
q code
acast
atunwa
audioboom
audiomack
audiomob
bigradio
blogtalkradio
idobiradio
lautfm
openstream
puradsifm
pureplayradio

radiocity

redcircle

spreaker

trinityaudio

tunein
zenomedia



*/
    
