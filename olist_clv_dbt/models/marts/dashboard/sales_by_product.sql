{{ config(
    materialized='view'
) }}

WITH orders AS (
    SELECT
        o.order_id,
        DATE_TRUNC(DATE(o.order_purchase_timestamp), MONTH) AS order_month,
        c.customer_state,
        oi.product_id,
        COALESCE(t.product_category_name_english, p.product_category_name) AS product_category_name,
        pay.payment_value
    FROM {{ ref('stg_orders') }} o
    JOIN {{ ref('stg_order_items') }} oi USING (order_id)
    JOIN {{ ref('stg_products') }} p USING (product_id)
    LEFT JOIN {{ ref('stg_category_translation') }} t ON p.product_category_name = t.product_category_name
    JOIN {{ ref('stg_customers') }} c USING (customer_id)
    LEFT JOIN {{ ref('stg_payments') }} pay USING (order_id)
)

SELECT
    order_month,
    customer_state,
    product_category_name,
    product_id,
    ROUND(SUM(payment_value), 2) AS total_sales
FROM orders
GROUP BY order_month, customer_state, product_category_name, product_id
ORDER BY total_sales DESC