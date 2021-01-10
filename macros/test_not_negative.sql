{% macro test_not_negative (model, column_name) %}
with validation as (
    select
        {{ column_name }} as quantity
    from {{ model }}
),
validation_errors as (
    select
        quantity
    from validation
    where quantity < 0
)
select count(*)
from validation_errors
{% endmacro %}
