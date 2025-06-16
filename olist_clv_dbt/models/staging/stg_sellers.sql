-- models/staging/stg_sellers.sql

SELECT
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state
FROM {{ source('raw', 'public_olist_sellers_dataset') }}