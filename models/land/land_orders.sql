{{ config(materialized='incremental',  transient=false) }}

with source as (select * from {{ source('fakedata', 'orders') }})
select
	a:datetime::datetime as datetime,
	a:order_id::int as order_id,
	a:customer_id::int as customer_id,
	a:product_id::int as product_id,
	a:quantity::int as quantity,
	filename::varchar as filename,
	date_inserted::datetime as date_inserted
from source

{% if is_incremental() %}
where date_inserted > (select max(date_inserted) from {{ this }})
{% endif %}