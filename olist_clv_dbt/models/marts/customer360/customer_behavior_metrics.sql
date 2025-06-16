WITH customer_orders AS (
    SELECT
        o.customer_id,
        d.customer_state,
        d.customer_city,
        DATE_TRUNC(o.order_purchase_timestamp, MONTH) AS order_month,
        COUNT(o.order_id) AS order_count,
        SUM(oi.price + oi.freight_value) AS total_spend,
        SUM(CASE WHEN o.order_status = 'delivered' THEN 1 ELSE 0 END) AS delivered_count
    FROM {{ ref('stg_orders') }} o
    JOIN {{ ref('stg_order_items') }} oi
        ON o.order_id = oi.order_id
    JOIN {{ ref('dim_customers') }} d
        ON o.customer_id = d.customer_id
    GROUP BY o.customer_id, d.customer_state, d.customer_city, order_month
),

aggregated_behavior AS (
    SELECT
        order_month,
        customer_state,
        customer_city,
        COUNT(DISTINCT customer_id) AS total_customers,
        SUM(total_spend) AS total_spend,
        AVG(total_spend) AS avg_spend_per_customer,
        SUM(delivered_count) / NULLIF(SUM(order_count), 0) AS pct_orders_completed
    FROM customer_orders
    GROUP BY order_month, customer_state, customer_city
)

SELECT * FROM aggregated_behavior
