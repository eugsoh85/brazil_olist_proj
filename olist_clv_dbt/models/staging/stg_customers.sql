-- models/staging/stg_customers.sql

SELECT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state
FROM {{ source('raw', 'public_olist_customers_dataset') }}