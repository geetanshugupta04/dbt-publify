with
    ssp_app_metadata as (
        select

            id,
            name as ssp_app_name,
            ssp_app_id,
            ssp,
            bundle,
            domain,
            platform_type,
            app_id as publify_app_id,
            categories,
            publisher_id

        from paytunes_data.metadata_sspapp

    )
select *
from ssp_app_metadata
