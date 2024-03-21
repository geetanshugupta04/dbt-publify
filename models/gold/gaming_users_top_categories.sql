with
gaming_users as (

  select 
  ip,
  -- , ifa
  -- , 
  -- uid, 
  user_id, 
  ssp, ad_type, sum(bids)
   from dbt_paytunes_gold.gaming_users
where 
gaming_category = 1 
and 
date = '2024-03-11'
and user_id not in ('0000-0000', '00000000-0000-0000-0000-000000000000')
group by 1,2
,3
,4
-- ,5
order by 
1
-- ,2
-- -- ,3
-- 4 desc


)

select 
-- make
-- ,
--  model
-- ,
-- * from gaming_users 
-- where user_id in (select user_id from (select user_id, count(distinct ssp) as counts from gaming_users group by 1 having counts > 2))
-- and ssp = 'Rubicon'
-- order by user_id, ssp
ad_type,
app_category_tag
, 
-- case when iab_category = 'NA' or iab_category ilike '%music%' or iab_category in ('Contemporary Hits/Pop/Top 40') then 'Music and Audio'
-- else iab_category 
-- end as 
-- content_category
-- , 
-- device_os,
-- -- dealcode,
-- ssp,
-- date,
-- gaming_category,
count(distinct user_id) as user_counts
-- ,
-- count(distinct ip) as ip_counts
-- -- round((count(distinct uid) / 764290)*100, 2) as uid_percent,
-- -- round((count(distinct ip) / 607345)*100, 2) as ip_percent


from 
dbt_paytunes_gold.gaming_users
where 
(
-- ip in (select ip from gaming_users)
-- -- or 
-- -- ifa in (select ifa from gaming_users)
-- -- or 
user_id in (select user_id from gaming_users)
)
and
gaming_category = 0
and 
date = '2024-03-11'

group by 1
,2
-- , 3
-- ,4,5
-- having ip_counts > 1000 or uid_counts > 1000
order by 1,3 desc

