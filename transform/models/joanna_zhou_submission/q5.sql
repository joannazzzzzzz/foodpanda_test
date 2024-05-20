-- case about using ref function 

{{
    config(materialized="view")
}}

-- question: select vendor in model q4 whoes name start with "I".
-- Supply a SQL header:
{% call set_sql_header(config) %}
  CREATE TEMPORARY FUNCTION start_with_i_boolean(vendor_name STRING)
  RETURNS BOOLEAN AS (
    case when lower(vendor_name) like '%i' then true else false end
  );
{%- endcall %}

select vendor_name, start_with_i_boolean(vendor_name) as is_start_with_i from {{ ref('q4') }} 