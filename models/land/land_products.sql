{{ config(materialized='incremental',  transient=false, unique_key='product_id') }}

with source as (select * from {{ source('fakedata', 'products') }})
select
	a:product_id::int as product_id,
	a:price::decimal as price,
	filename::varchar as filename,
	date_inserted::datetime as date_inserted
from source
{% if is_incremental() %}
where date_inserted > (select max(date_inserted) from {{ this }})
{% endif %}
qualify row_number() over (partition by product_id order by date_inserted desc) = 1