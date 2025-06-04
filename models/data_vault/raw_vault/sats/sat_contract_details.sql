{{ config(materialized='incremental') }}

{%- set source_model = "v_stg_contracts_44" -%}
{%- set src_pk = "CONTRACT_HK" -%}
{%- set src_hashdiff = "CONTRACT_HASHDIFF" -%}
{%- set src_payload = [
    "regnum", "number", "sign_date", "execution_start_date", "execution_end_date", "publish_date", "price",
    "currency", "eis_url", "purchase_code", "single_supplier_reason_code", "single_supplier_reason_name", "version_number", "region_code"
] -%}
{%- set src_eff = "EFFECTIVE_FROM" -%}
{%- set src_ldts = "LOAD_DATE" -%}
{%- set src_source = "RECORD_SOURCE" -%}
{{ automate_dv.sat(
    src_pk=src_pk, src_hashdiff=src_hashdiff, src_payload=src_payload,
    src_eff=src_eff, src_ldts=src_ldts, src_source=src_source, source_model=source_model
) }}
