{{ config(
    materialized='view'
) }}

WITH orders AS (
    SELECT
        o.order_id,
        o.customer_id,
        c.customer_unique_id,
        DATE_TRUNC(DATE(o.order_purchase_timestamp), MONTH) AS order_month,
        p.payment_value
    FROM {{ ref('stg_orders') }} o
    LEFT JOIN {{ ref('stg_payments') }} p USING (order_id)
    LEFT JOIN {{ ref('stg_customers') }} c USING (customer_id)
    WHERE o.order_status IN ('delivered', 'shipped')
)

SELECT
    order_month,
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(DISTINCT customer_unique_id) AS total_customers,
    ROUND(SUM(payment_value), 2) AS total_revenue,
    ROUND(SUM(payment_value) / COUNT(DISTINCT order_id), 2) AS avg_order_value,
    ROUND(COUNT(DISTINCT order_id) / COUNT(DISTINCT customer_unique_id), 2) AS orders_per_customer
FROM orders
GROUP BY order_month
ORDER BY order_month