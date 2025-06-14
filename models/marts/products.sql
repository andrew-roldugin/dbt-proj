{{ config(
    materialized='incremental',
    unique_key='contract_id || sid',
    incremental_strategy='append'
) }}

WITH source AS (
    SELECT
        contract_id,
        sid,
        name,
         toFloat64(price) AS price,
        -- Если quantity отсутствует, но есть price, подставляем 1
        toFloat64(
            CASE 
                WHEN quantity IS NULL AND price IS NOT NULL THEN 1
                ELSE quantity
            END
        ) AS quantity,
        -- Если sum_total отсутствует, считаем как price * quantity
        toFloat64(
            CASE 
                WHEN sum_total IS NULL THEN 
                    toFloat64(price) * 
                    CASE 
                        WHEN quantity IS NULL AND price IS NOT NULL THEN 1
                        ELSE toFloat64(quantity)
                    END
                ELSE toFloat64(sum_total)
            END
        ) AS sum_total,
        okpd2_code,
        okpd2_name,
        okei_code,
        okei_name,
        updated_at
    FROM postgresql('194.147.87.105:5432', 'zakupki', 'stg_products', 'dbt_user', 'AE%lJV7RijQ5Zk', 'stg')
    {% if is_incremental() %}
        WHERE updated_at > (SELECT max(updated_at) FROM {{ this }})
    {% endif %}
)

SELECT * FROM source