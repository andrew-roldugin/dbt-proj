{{ config(materialized='incremental') }}

{%- set source_model = "v_stg_products" -%}
{%- set src_pk = "PRODUCT_HK" -%}
{%- set src_hashdiff = "PRODUCT_HASHDIFF" -%}
{%- set src_payload = [
    "name", "price", "quantity", "sum_total", "okpd2_code", "okpd2_name", "okei_code", "okei_name"
] -%}
{%- set src_eff = "EFFECTIVE_FROM" -%}
{%- set src_ldts = "LOAD_DATE" -%}
{%- set src_source = "RECORD_SOURCE" -%}
{{ automate_dv.sat(
    src_pk=src_pk, src_hashdiff=src_hashdiff, src_payload=src_payload,
    src_eff=src_eff, src_ldts=src_ldts, src_source=src_source, source_model=source_model
) }}
