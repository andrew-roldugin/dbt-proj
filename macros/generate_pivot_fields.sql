{% macro generate_pivot_fields(target_table) %}
    {%- set fields = dbt_utils.get_column_values(
        table=source('core', 'field_mapping'),
        column='target_field',
        where="target_table = '" ~ target_table ~ "'"
    ) -%}

    {%- for field in fields %}
        max(case when target_field = '{{ field }}' then element_value end) as {{ field }}{% if not loop.last %},{% endif %}
    {%- endfor %}
{% endmacro %}
