{{
  config(
    materialized = 'incremental',
    unique_key = 'customer_surrogate_key',
    incremental_strategy = 'append', 
    schema='stg'
    )
}}

with raw as (
    select * from {{ source('core', 'raw_contracts_eav') }}
    where path ~ '^customer.'
),
pivoted as (
    select
        contract_id,
        max(case when path = 'customer.fullName' then element_value end) as full_name,
        max(case when path = 'customer.inn' then element_value end) as inn,
        max(case when path = 'customer.kpp' then element_value end) as kpp,
        max(case when path = 'customer.okpo' then element_value end) as okpo,
        max(case when path = 'customer.region_code' then element_value end) as region_code,
        max(case when path = 'customer.region_name' then element_value end) as region_name,
        max(case when path = 'customer.post_address' then element_value end) as post_address
    from raw
    group by contract_id
)
select 
    {{ dbt_utils.generate_surrogate_key(['p.inn', 'p.kpp']) }} as customer_surrogate_key,
    p.*
 from pivoted p