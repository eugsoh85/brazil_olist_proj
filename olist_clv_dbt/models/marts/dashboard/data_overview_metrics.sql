{{ config(
    materialized='view'
) }}

WITH orders AS (
    SELECT
        o.order_id,
        o.customer_id,
        o.order_purchase_timestamp,
        FORMAT_DATE('%Y-%m', DATE(o.order_purchase_timestamp)) AS order_month,
        oi.product_id,
        p.product_category_name,
        s.seller_id,
        c.customer_state
    FROM {{ ref('stg_orders') }} o
    JOIN {{ ref('stg_order_items') }} oi USING (order_id)
    JOIN {{ ref('stg_products') }} p USING (product_id)
    JOIN {{ ref('stg_sellers') }} s USING (seller_id)
    JOIN {{ ref('stg_customers') }} c USING (customer_id)
),

-- Monthly Sales
monthly_sales AS (
    SELECT
        order_month,
        COUNT(DISTINCT order_id) AS total_orders,
        COUNT(DISTINCT customer_id) AS total_customers
    FROM orders
    GROUP BY order_month
),

-- Product Stats
product_stats AS (
    SELECT
        product_id,
        COUNT(*) AS product_order_count
    FROM orders
    GROUP BY product_id
),

-- Category Stats
category_stats AS (
    SELECT
        product_category_name,
        COUNT(*) AS category_order_count
    FROM orders
    GROUP BY product_category_name
),

-- Sellers Per Category
seller_per_category AS (
    SELECT
        product_category_name,
        seller_id
    FROM orders
),

-- Orders Per State
orders_per_state AS (
    SELECT
        customer_state,
        COUNT(*) AS state_order_count
    FROM orders
    GROUP BY customer_state
)

SELECT
    ms.order_month,
    ms.total_orders,
    ms.total_customers,
    
    ps.product_id,
    ps.product_order_count,

    cs.product_category_name,
    cs.category_order_count,

    spc.product_category_name AS seller_category,
    spc.seller_id,

    os.customer_state,
    os.state_order_count

FROM monthly_sales ms
LEFT JOIN product_stats ps ON 1=1
LEFT JOIN category_stats cs ON 1=1
LEFT JOIN seller_per_category spc ON 1=1
LEFT JOIN orders_per_state os ON 1=1