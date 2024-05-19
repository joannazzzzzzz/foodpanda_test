{{
    config(
        materialized='view'
        incremental_strategy='insert_overwrite'
        partitioned_by=['vendor_name']
    )
}}

-- 2. calculate the GMV of vendors in Taiwan and order the result their customer count
with tw_orders as (
  select *
  from `orders.orders_info`
  where country_name = 'Taiwan'
)

select 
  b.vendor_name
  , count(distinct a.customer_id) as customer_count
  , Round(sum(a.gmv_local),2) as total_gmv
from tw_orders a
left join `vendor.vendor_info` b 
  on a.country_name = b.country_name 
  and a.rdbms_id = b.rdbms_id 
  and a.vendor_id = b.id
group by vendor_name
order by customer_count desc
;