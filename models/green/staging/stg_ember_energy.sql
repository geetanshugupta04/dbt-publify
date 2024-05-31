with

    yearly_energy_by_country as (
        select

            `Area` as area,
            `Country code` as country_code,
            date(cast(`Year` as string)) as year,
            `Area type` as area_type,
            `Continent` as continent,
            `Ember region` as ember_region,
            `EU` as is_eu,
            `OECD` as is_oecd,
            `G20` as is_g20,
            `G7` as is_g7,
            `ASEAN` as is_asean,
            `Category` as category,
            `Subcategory` as subcategory,
            `Variable` as variable,
            `Unit` as unit,
            `Value` as value,
            `YoY absolute change` as yoy_absolute_change,
            `YoY % change` as yoy_percentage_change

        from hive_metastore.default.yearly_energy_by_country

    )

select *
from yearly_energy_by_country
