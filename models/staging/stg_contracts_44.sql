{{
  config(
    materialized = 'incremental',
    unique_key = ['id'],
    incremental_strategy = 'append', 
    schema='stg'
    )
}}
with raw as (
    select * from {{ source('core', 'raw_contracts_eav') }}
    where path in (
        'id', 'regNum', 'number', 'signDate', 'execution.startDate', 'execution.endDate',
        'publishDate', 'price', 'currency.code', 'contractUrl', 'foundation.fcsOrder.purchaseCode',
        'singleCustomerReason.code', 'singleCustomerReason.name', 'versionNumber', 'regionCode'
    )
),
pivoted as (
    select
        contract_id,
        max(case when path = 'id' then element_value end) as id,
        max(case when path = 'regNum' then element_value end) as regnum,
        max(case when path = 'number' then element_value end) as number,
        max(case when path = 'signDate' then element_value end)::date as sign_date,
        max(case when path = 'execution.startDate' then element_value end)::date as execution_start_date,
        max(case when path = 'execution.endDate' then element_value end)::date as execution_end_date,
        max(case when path = 'publishDate' then element_value end)::timestamp as publish_date,
        max(case when path = 'price' then element_value end)::numeric as price,
        max(case when path = 'currency.code' then element_value end) as currency,
        max(case when path = 'contractUrl' then element_value end) as eis_url,
        max(case when path = 'foundation.fcsOrder.purchaseCode' then element_value end) as purchase_code,
        max(case when path = 'singleCustomerReason.code' then element_value end) as single_supplier_reason_code,
        max(case when path = 'singleCustomerReason.name' then element_value end) as single_supplier_reason_name,
        max(case when path = 'versionNumber' then element_value end)::int as version_number,
        max(case when path = 'regionCode' then element_value end) as region_code
    from raw
    group by contract_id
)
select * from pivoted