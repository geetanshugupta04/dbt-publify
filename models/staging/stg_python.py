def model(dbt, session):

    dbt.config(materialized="table")

    df = dbt.ref("stg_impressions")

    return df