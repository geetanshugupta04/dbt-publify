with 
impressions as (

    select * from {{ ref('stg_python')}}
)

select * from impressions