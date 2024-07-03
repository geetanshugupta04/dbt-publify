with

    energy_data as (select * from {{ ref("stg_ember_energy") }}),

    wb_cbam_data as (select * from {{ ref("stg_wb_cbam_exposure_index") }}),

    energy_data_countries as (

        select country_code, continent, ember_region, is_eu

        from energy_data
        where area_type = 'Country'
        group by all
        order by all
    ),

    joined as (

        select
            wb_cbam_data.country,
            wb_cbam_data.iso3_country_code,
            energy_data_countries.continent,
            energy_data_countries.ember_region,
            energy_data_countries.is_eu,
            wb_cbam_data.cbam_exports_percent_world,
            wb_cbam_data.cbam_relative_carbon_intensity,
            wb_cbam_data.cbam_exports_percent_gdp,
            wb_cbam_data.aggregate_relative_cbam_index

        from wb_cbam_data
        left join
            energy_data_countries
            on wb_cbam_data.iso3_country_code = energy_data_countries.country_code

    )

select *
from joined
