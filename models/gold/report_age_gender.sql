with

    track_age_gender as (
        select *
        from {{ ref("int_age_gender") }}
        where campaign_name = 'APOLLA210220241'
    -- and age = 'unspecified'
    ),

    track_age_gender_existance as (
        select
            t.*,
            case
                when t.age in ('13-17', '35-44', '45+', '25-34', '18-24')
                then 1
                when t.age = 'unspecified' or t.age is null
                then 0

            end as age_exists
        from track_age_gender as t

    ),

    track_age_gender_exists as (

        select campaign_name, sum(impression) as sum_impression_exists

        from track_age_gender_existance
        where age_exists = 1
        group by 1

    ),

    track_age_gender_na as (

        select campaign_name, sum(impression) as sum_impression_na

        from track_age_gender_existance
        where age_exists = 0
        group by 1

    ),

    track_sums_merged as (

        select track.*, track_exists.sum_impression_exists, track_na.sum_impression_na

        from track_age_gender_existance as track
        
        left join
            track_age_gender_exists as track_exists
            on track.campaign_name = track_exists.campaign_name
        left join
            track_age_gender_na as track_na
            on track.campaign_name = track_na.campaign_name
        where track.age_exists = 1
    )

-- -- all 3 cases exist
-- track_age_gender_specified as (
-- select campaign_name, sum(impression) 
-- from track_age_gender 
-- where exists ()
-- )
-- track_age_gender_null_only as (
-- select 
-- )
-- track_age_gender_all_possibilites as (
-- )
select *
from track_sums_merged
order by campaign_name, gender
