{% macro clean_triton_pubs(bids) %}

    {% set triton_pubs = [
        "accuradio",
        "all_radio",
        "audacy",
        "audioboom",
        "audiomob",
        "adtonos",
        "blogtalkradio",
        "bloomberg",
        "cumulus",
        "edisound",
        "entravision",
        "esound",
        "gadsme",
        "grupo",
        "instreamatic",
        "libsyn",
        "nova",
        "odeeo",
        "omny",
        "pocketfm",
        "podbean",
        "primedia",
        "spreaker",
        "srgstr",
        "talpa",
        "targetspot",
        "trinity",
        "triton_podcast",
        "videobyte",
        "zenoads",
        "zenofon",
    ] %}

    {% set triton_pubs_mapping = {
        "entertainment network india limited": "entertainment_network",
        "liberated syndication inc": "libsync",
        "pocket fm pvt ltd": "pocketfm",
        "spicy sparks s.r.l": "spicy_sparks",
        "tim media": "trinity",
        "zeno radio": "zenoradio",
        1113: "grupo",
    } %}

    select
        ssp,
        ssp_app_name,

        case
            {% for pub in triton_pubs %}
                when ssp_app_name ilike '%{{pub}}%' then '{{pub}}'
            {% endfor %}
            else lower(ssp_app_name)
        end as publisher_cleaned

    from {{ bids }}
    where ssp = 'Triton'

{% endmacro %}
