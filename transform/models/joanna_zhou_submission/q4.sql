{{
    config(
        materialized='view'
        incremental_strategy='insert_overwrite'
        partitioned_by={'field': 'year', 'data_type': 'datetime'}
    )
}}

-- 4. find the top 2 vendors per country, in each year available in the dataset
-- here assume the rank is by total GMV
select 
  year_local as year
  , country_name
  , vendor_name
  , total_gmv
from (
  select 
    year_local
    , country_name
    , vendor_name
    , total_gmv
    , rank() over (partition by year_local, country_name order by total_gmv desc) as gmv_rank
  from (
    select 
    DATE_TRUNC(DATETIME(EXTRACT(year FROM a.date_local), EXTRACT(month FROM a.date_local), EXTRACT(day FROM a.date_local), 00, 00, 00), year) as year_local
    , a.country_name
    , b.vendor_name
    , Round(sum(a.gmv_local),2) as total_gmv
    from `orders.orders_info` a
    left join `vendor.vendor_info` b 
      on a.country_name = b.country_name 
      and a.rdbms_id = b.rdbms_id 
      and a.vendor_id = b.id
    group by 
    year_local
    , a.country_name
    , b.vendor_name
    )
  )
where gmv_rank <= 2
order by 
  year
  , country_name
  , total_gmv desc
;
