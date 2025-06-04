{{ config(materialized='incremental') }}

{%- set source_model = "v_stg_suppliers" -%}
{%- set src_pk = "SUPPLIER_HK" -%}
{%- set src_hashdiff = "SUPPLIER_HASHDIFF" -%}
{%- set src_payload = [
    "full_name", "inn", "kpp", "okpo", "region_code", "region_name", "address", "is_unfair"
] -%}
{%- set src_eff = "EFFECTIVE_FROM" -%}
{%- set src_ldts = "LOAD_DATE" -%}
{%- set src_source = "RECORD_SOURCE" -%}
{{ automate_dv.sat(
    src_pk=src_pk, src_hashdiff=src_hashdiff, src_payload=src_payload,
    src_eff=src_eff, src_ldts=src_ldts, src_source=src_source, source_model=source_model
) }}
