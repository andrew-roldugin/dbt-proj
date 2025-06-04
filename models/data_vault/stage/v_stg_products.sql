{{ config(materialized='view') }}

{%- set yaml_metadata -%}
source_model: "stg_products"
derived_columns:
  RECORD_SOURCE: "'zakupki'"
  LOAD_DATE: "name"
  EFFECTIVE_FROM: "name"
hashed_columns:
  PRODUCT_HK: "okpd2_code"
  PRODUCT_HASHDIFF:
    is_hashdiff: true
    columns:
      - "name"
      - "okpd2_code"
      - "price"
      - "quantity"
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
