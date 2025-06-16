{{ config(
    materialized='view'
) }}

SELECT
    product_category_name,
    product_category_name_english
FROM {{ source('raw', 'public_product_category_name_translation') }}