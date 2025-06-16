WITH delivery_data AS (
    SELECT
        o.order_id,
        o.customer_id,
        DATE_TRUNC(o.order_purchase_timestamp, MONTH) AS order_month,
        DATE_DIFF(
            o.order_delivered_carrier_date,
            o.order_purchase_timestamp,
            DAY
        ) AS delivery_days
    FROM {{ ref('stg_orders') }} o
    WHERE o.order_delivered_customer_date IS NOT NULL
)

SELECT
    order_month,
    COUNT(DISTINCT customer_id) AS total_customers,
    AVG(delivery_days) AS avg_delivery_days
FROM delivery_data
GROUP BY order_month
