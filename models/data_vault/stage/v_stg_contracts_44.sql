{{ config(materialized='view') }}

{%- set yaml_metadata -%}
source_model: "stg_contracts_44"
derived_columns:
  RECORD_SOURCE: "'zakupki'"
  LOAD_DATE: "publish_date"
  EFFECTIVE_FROM: "sign_date"
hashed_columns:
  CONTRACT_HK: "id"
  CONTRACT_HASHDIFF:
    is_hashdiff: true
    columns:
      - "regnum"
      - "number"
      - "sign_date"
      - "price"
      - "currency"
      - "purchase_code"
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
