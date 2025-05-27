{% macro generate_stg_model(family_name) %}

  {% set mappings_query %}
    select
      fm.source_field,
      fm.target_table,
      fm.target_field,
      fm.semantic_data_type,
      xs.xpath
    from {{ source('core', 'field_mapping') }} fm
    join {{ source('core', 'xsd_specification') }} xs on xs.spec_id = fm.specification_id
    join {{ source('core', 'xsd_specification_details') }} xsd on xs.spec_id = xsd.specification_id
    join {{ source('core', 'document_family') }} d on xsd.family_id = d.family_id
    where d.family_name = '{{ family_name }}'
      and xsd.is_active
  {% endset %}

  {% set mappings = run_query(mappings_query) %}

  {% set columns = mappings.columns %}
  {% set rows = mappings.rows %}
  {% set mappings = [] %}
  {% for row in rows %}
    {% set mapping = {} %}
    {% for col_idx in range(columns | length) %}
      {% do mapping.update({ (columns[col_idx].name): row[col_idx] }) %}
    {% endfor %}
    {% do mappings.append(mapping) %}
  {% endfor %}

  {% if mappings | length == 0 %}
    {{ log("No mappings found for family: " ~ family_name, info=True) }}
    {% do exceptions.raise_compiler_error("No mappings found for family: " ~ family_name) %}
  {% endif %}

  {% set main_table = (mappings | map(attribute='target_table') | list)[0] %}
  {% set all_sql = [] %}

  {% for table in mappings | map(attribute='target_table') | unique %}
    {{ log("Generating model for: " ~ table, info=True) }}
    {% set table_mappings = mappings | selectattr('target_table', 'equalto', table) | list %}

    {% set is_main = table == main_table %}
    {% set where_clause = 'length(r.record_id) = 3' if is_main else "r.record_id like '___/%'" %}

    {% set sql %}
-- Model: {{ table }}
with raw as (
  select
    r.file_id,
    r.record_id,
    {% if not is_main %}substring(r.record_id from 1 for 3) as parent_record_id,{% endif %}
    {% for row in table_mappings %}
    max(case when r.element_name = '{{ row.source_field }}' then r.element_value end) as {{ row.target_field }}{% if not loop.last %},{% endif %}
    {% endfor %}
  from {{ source('core', 'raw_xml') }} r
  join {{ source('core', 'file_metadata') }} f on r.file_id = f.file_id
  where f.family_id = (
      select family_id from {{ source('core', 'document_family') }}
      where family_name = '{{ family_name }}'
      limit 1
  )
    and {{ where_clause }}
  group by r.file_id, r.record_id{% if not is_main %}, parent_record_id{% endif %}
)

select
  *{% if not is_main %},
  parent_record_id as {{ main_table }}_record_id{% endif %}
from raw;
    {% endset %}

    {% do all_sql.append(sql) %}
  {% endfor %}

  {{ log(all_sql | join('\n\n'), info=True) }}

{% endmacro %}
