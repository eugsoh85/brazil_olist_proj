WITH customers AS (
    SELECT
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state
    FROM {{ ref('stg_customers') }}
)

SELECT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    INITCAP(customer_city) AS customer_city,
    UPPER(customer_state) AS customer_state
FROM customers