{{ config(materialized='view') }}

{%- set yaml_metadata -%}
source_model: "stg_customers"
derived_columns:
  RECORD_SOURCE: "'zakupki'"
  LOAD_DATE: "now()"
  EFFECTIVE_FROM: "now()::date"
hashed_columns:
  CUSTOMER_HK: "inn"
  CUSTOMER_HASHDIFF:
    is_hashdiff: true
    columns:
      - "full_name"
      - "inn"
      - "kpp"
      - "post_address"
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
