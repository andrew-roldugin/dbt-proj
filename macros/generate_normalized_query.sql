{% macro generate_normalized_query(target_table) %}
{# Получаем все поля и типы по заданной таблице #}
{%- call statement('get_fields', fetch_result=True) -%}
    select target_field, semantic_data_type
    from {{ source('core', 'field_mapping') }}
    where target_table = '{{ target_table }}'
{%- endcall -%}
{%- set rows = load_result('get_fields')['data'] %}

{%- if rows | length == 0 %}
    select file_id from {{ source('core', 'raw_xml') }} where false -- no fields found
{%- else %}

(with eav as (
    select
        record_id,
        file_id,
        fm.target_field,
        fm.semantic_data_type,
        rx.element_value,
        rx.parent_id,
        depth,
        path
    from {{ source('core', 'raw_xml') }} rx
    join {{ source('core', 'xsd_specification') }} xs 
        on xs.xpath = rx.path
    join {{ source('core','field_mapping') }} fm 
        on xs.spec_id = fm.specification_id
        and fm.source_field = rx.element_name
    join {{ source('core', 'xsd_specification_details') }} xsd
        on xsd.specification_id = xs.spec_id
    where fm.target_table = '{{ target_table }}'
      and rx.type = 'element'
      and xsd.is_active
),

pivoted as (
    select
        file_id,
        min(record_id) as record_id,
        parent_id,
        depth,
        min(path) as path,
        {%- for row in rows %}
            {%- set field = row[0] %}
            {%- set dtype = row[1] | lower %}
            max(case when target_field = '{{ field }}' then element_value end)
                {% if dtype == 'integer' %}::int8
                {% elif dtype == 'float' %}::float
                {% elif dtype == 'boolean' %}::boolean
                {% elif dtype == 'date' %}::date
                {% elif dtype == 'datetime' %}::timestamp
                {% endif %}
            as {{ field }}{% if not loop.last %},{% endif %}
        {%- endfor %}
    from eav
    group by file_id, parent_id, depth
)

select *,
    md5(concat_ws(';', file_id, split_part(record_id, E'\\', depth - 1),
                 regexp_replace(path, '\\[^\\]+$', '')))      as surrogate_pk,
    case when depth - 2 > 0 then 
        md5(concat_ws(';', file_id, split_part(parent_id, E'\\', depth - 2),
        regexp_replace(path, '(\\[^\\]+){2}$', '')))
    end as surrogate_fk
from pivoted) as {{ target_table }}

{%- endif %}
{% endmacro %}
