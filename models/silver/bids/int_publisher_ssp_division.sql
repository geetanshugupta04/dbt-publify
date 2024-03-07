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
    "trinityaudio",
    "tunein",
    "zenomedia",
] %}


with
    ssp_apps_bids as (select * from {{ ref("int_apps_across_ssps") }}),

    -- cleaned_pubs_triton as (
    -- select
    -- ssp_app_name,
    -- case
    -- {% for pub in triton_pubs %}
    -- when ssp_app_name ilike '%{{pub}}%' then '{{pub}}'
    -- {% endfor %}
    -- else ssp_app_name
    -- end as publisher_cleaned,
    -- sum(bid_counts) as bids
    -- from ssp_apps_bids
    -- where ssp = 'Triton'
    -- group by 1, 2
    -- having bids > 1000
    -- order by 2
    -- ),
    -- cleaned_pubs_adswizz as (

    --     select
    --         ssp_app_name,
    --         -- case
    --         -- {% for pub in triton_pubs %}
    --         -- when ssp_app_name ilike '%{{pub}}%' then '{{pub}}'
    --         -- {% endfor %}
    --         -- else ssp_app_name
    --         -- end as publisher_cleaned,
    --         sum(bid_counts) as bids
    --     from ssp_apps_bids
    --     where ssp = 'Adswizz'
    --     group by 1
    --     having bids > 100
    --     order by 1

    -- )

    cleaned_pubs_rubicon as (

        select 
            publisher_id,
            ssp_app_name,
            publify_app_name,
            avg_floor_price,
            -- case
            -- {% for pub in triton_pubs %}
            -- when ssp_app_name ilike '%{{pub}}%' then '{{pub}}'
            -- {% endfor %}
            -- else ssp_app_name
            -- end as publisher_cleaned,
            sum(bid_counts) as bids
            
        from ssp_apps_bids
        where ssp = 'Rubicon' and ad_type in ('audio', 'podcast') and publify_app_name not in ('wynk')
        group by 1,2,3,4
        having bids > 10
        order by 1

    )

select *
from
    cleaned_pubs_rubicon

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
    
