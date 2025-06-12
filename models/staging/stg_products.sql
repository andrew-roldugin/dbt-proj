{{
  config(
    materialized = 'incremental',
    unique_key = ['contract_id', 'sid'],
    incremental_strategy = 'append', 
    schema='stg'
    )
}}

with raw as (
    select 
        *,
        (regexp_match(path, '^products\[\d+\]'))[1] as enumerate_item,
	    regexp_replace(path, '^products\[\d+\].', '') as element_path
    from {{ source('core', 'raw_contracts_eav') }}
    where path ~ '^products\[\d+\].'
),
pivoted as (
    select
        contract_id,
        max(case when element_path ~ '^sid' then element_value end) as sid,
        max(case when element_path ~ '^name' then element_value end) as name,
        max(case when element_path ~ '^price' then element_value end)::numeric as price,
        max(case when element_path ~ '^quantity' then element_value end)::numeric as quantity,
        max(case when element_path ~ '^sum' then element_value end)::numeric as sum_total,
        max(case when element_path ~ 'OKPD2.code$' then element_value end) as okpd2_code,
        max(case when element_path ~ 'OKPD2.name$' then element_value end) as okpd2_name,
        max(case when element_path ~ 'OKEI.code$' then element_value end) as okei_code,
        max(case when element_path ~ 'OKEI.name$' then element_value end) as okei_name
    from raw
    group by contract_id, enumerate_item
)
select * from pivoted