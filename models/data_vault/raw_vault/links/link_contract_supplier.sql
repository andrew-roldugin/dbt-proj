{{ config(materialized='incremental') }}

{%- set source_model = "v_stg_suppliers" -%}
{%- set src_pk = "inn" -%}
{%- set src_fk = ["CONTRACT_HK", "SUPPLIER_HK"] -%}
{%- set src_ldts = "LOAD_DATE" -%}
{%- set src_source = "RECORD_SOURCE" -%}
{{ automate_dv.link(
    src_pk=src_pk, src_fk=src_fk, src_ldts=src_ldts, src_source=src_source, source_model=source_model
) }}
