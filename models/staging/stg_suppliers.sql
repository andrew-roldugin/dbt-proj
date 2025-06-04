{{
  config(
    materialized = 'incremental',
    unique_key = ['inn', 'kpp'],
    incremental_strategy = 'append', 
    schema='stg'
    )
}}

with raw as (
    select * from {{ source('core', 'raw_contracts_eav') }}
    where path ~ '^suppliers\[\d+\].'
),
pivoted as (
    select
        {{ dbt_utils.generate_surrogate_key(['inn', 'knn']) }} as supplier_surrogate_key,
        contract_id,
        max(case when path ~ 'organizationName$' then element_value end) as organization_name,
        max(case when path ~ 'inn$' then element_value end) as inn,
        max(case when path ~ 'kpp$' then element_value end) as kpp,
        max(case when path ~ 'okpo$' then element_value end) as okpo,
        max(case when path ~ 'ogrn$' then element_value end) as ogrn,
        concat_ws(' ',
            max(case when path ~ 'contactInfo.lastName$' then element_value end),
            max(case when path ~ 'contactInfo.firstName$' then element_value end),
            max(case when path ~ 'contactInfo.middleName$' then element_value end)
        ) as supplier_full_name,
        max(case when path ~ 'region_code$' then element_value end) as region_code,
        max(case when path ~ 'region_name$' then element_value end) as region_name,
        max(case when path ~ 'factualAddress$' then element_value end) as factual_address,
        max(case when path ~ 'postAddress$' then element_value end) as post_address,
        max(case when path ~ 'is_unfair$' then element_value end)::boolean as is_unfair
    from raw
    group by contract_id, (regexp_match(path, '^suppliers\[\d+\]'))[1]
)
select * from pivoted