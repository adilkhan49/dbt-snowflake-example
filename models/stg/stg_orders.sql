{{ config(materialized='view') }}

with orders as (select * from {{ ref('land_orders') }}),
products as (select * from {{ ref('land_products') }})

select 
	o.datetime,
	o.order_id,
	o.customer_id,
	o.product_id,
	o.quantity,
	p.price,
	o.quantity * p.price as amount
from orders o
join products p on o.product_id = p.product_id