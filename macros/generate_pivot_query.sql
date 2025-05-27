{% macro generate_pivot_query(target_table) %}
with
mapping as (
    select
        target_field,
        semantic_data_type
    from {{ source('core', 'field_mapping') }}
    where target_table = '{{ target_table }}'
),

eav as (
    select
        rx.record_id,
        rx.file_id,
        fm.target_field,
        fm.semantic_data_type,
        rx.element_value
    from {{ source('core', 'raw_xml') }} rx
    left join {{ source('core', 'xsd_specification') }} xs
        on xs.xpath = rx.path
    left join {{ source('core', 'field_mapping') }} fm
        on xs.spec_id = fm.specification_id
        and fm.source_field = rx.element_name
        and rx.type = 'element'
        and fm.target_table = '{{ target_table }}'
),

pivoted as (
    select
        file_id,
        {% for row in execute('select target_field, semantic_data_type from {{ ref("mapping_" ~ target_table) }}') %}
            max(case when target_field = '{{ row.target_field }}' then
                {% set dtype = row.semantic_data_type | lower %}
                {% if dtype == 'integer' %}
                    element_value::int
                {% elif dtype == 'float' %}
                    element_value::float
                {% elif dtype == 'boolean' %}
                    element_value::boolean
                {% elif dtype == 'date' %}
                    element_value::date
                {% elif dtype == 'datetime' %}
                    element_value::timestamp
                {% else %}
                    element_value
                {% endif %}
            end) as {{ row.target_field }}{% if not loop.last %},{% endif %}
        {% endfor %}
    from eav
    group by file_id
)

select * from pivoted
{% endmacro %}
