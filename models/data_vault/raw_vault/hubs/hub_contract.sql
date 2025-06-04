{{ config(materialized='incremental') }}

{%- set source_model = "v_stg_contracts_44" -%}
{%- set src_pk = "CONTRACT_HK" -%}
{%- set src_nk = "id" -%}
{%- set src_ldts = "LOAD_DATE" -%}
{%- set src_source = "RECORD_SOURCE" -%}

{{ automate_dv.hub(
    src_pk=src_pk, src_nk=src_nk, src_ldts=src_ldts,
    src_source=src_source, source_model=source_model
) }}
