{{
    config(
        materialized='view'
        incremental_strategy='insert_overwrite'
        partitioned_by=['country_name']
    )
}}

-- 3. find the top active vendor by GMV in each country
select 
  country_name
  , vendor_name
  , total_gmv
from (
  select 
    country_name
    , vendor_name
    , total_gmv
    , rank() over (partition by country_name order by total_gmv desc) as gmv_rank
  from (
    select 
    a.country_name
    , b.vendor_name
    , Round(sum(a.gmv_local),2) as total_gmv
    from `orders.orders_info` a
    left join `vendor.vendor_info` b 
      on a.country_name = b.country_name 
      and a.rdbms_id = b.rdbms_id 
      and a.vendor_id = b.id
    where b.is_active = true
    group by 
    a.country_name
    , b.vendor_name
    )
  )
where gmv_rank = 1
order by country_name
;