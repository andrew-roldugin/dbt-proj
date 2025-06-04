{{ config(materialized='view') }}

{%- set yaml_metadata -%}
source_model: "stg_suppliers"
derived_columns:
  RECORD_SOURCE: "'zakupki'"
  LOAD_DATE: "now()"
  EFFECTIVE_FROM: "now()::date"
hashed_columns:
  SUPPLIER_HK: "inn"
  SUPPLIER_HASHDIFF:
    is_hashdiff: true
    columns:
      - "supplier_full_name"
      - "inn"
      - "kpp"
      - "factual_address"
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(
    include_source_columns=true,
    source_model=metadata_dict['source_model'],
    derived_columns=metadata_dict['derived_columns'],
    null_columns=none,
    hashed_columns=metadata_dict['hashed_columns'],
    ranked_columns=none
) }}
