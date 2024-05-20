{{
    config(
        materialized='view'
        incremental_strategy='insert_overwrite'
        partitioned_by=['country_name']
    )
}}

-- 1. find the total GMV by country
select 
  country_name
  , Round(sum(gmv_local),2) as total_gmv
from `orders.orders_info`
group by 1
;

