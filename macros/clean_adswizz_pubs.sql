{% macro clean_adswizz_pubs(bids) %}

    {% set adswizz_pubs = [
        "acast",
        "airwave",
        "astro",
        "atunwa",
        "audioboom",
        "audiomack",
        "audiomob",
        "bigradio",
        "blogtalkradio",
        "empirestreaming",
        "gadsme",
        "idobiradio",
        "lautfm",
        "libsyn",
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

    select
        ssp,
        ssp_app_name,
        case
            {% for pub in adswizz_pubs %}
                when publisher_id ilike '%{{pub}}%' then '{{pub}}'
            {% endfor %}
            else lower(publisher_id)
        end as publisher_cleaned

    from {{ bids }}
    where ssp = 'Adswizz'

{% endmacro %}
