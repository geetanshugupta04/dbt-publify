with

    wb_cbam_exposure as (
        select

            country as country,
            iso3 as iso3_country_code,
            cast(
                split_part(
                    `CBAM products exports to the EU (% of total CBAM products exports to world)`,
                    '%',
                    1
                ) as float
            ) as cbam_exports_percent_world,
            `Trade weighted average *relative* potential carbon embodied payment per dollar of exports of covered goods to EU` cbam_relative_carbon_intensity,
            cast(
                split_part(
                    `CBAM products exports to the EU (% of GDP)`, '%', 1
                ) as float
            ) as cbam_exports_percent_gdp,
            `Aggregate relative CBAM exposure index` as aggregate_relative_cbam_index,
            `Most exposed CBAM products` as most_exposed_cbam_products

        from hive_metastore.default.wb_cbam_exposure
        where country is not null and country != 'Worst'

    )

select *
from wb_cbam_exposure
