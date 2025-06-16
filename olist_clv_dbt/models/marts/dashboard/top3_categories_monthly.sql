{{ config(
    materialized='view'
) }}

WITH orders AS (
    SELECT
        o.order_id,
        DATE_TRUNC(DATE(o.order_purchase_timestamp), MONTH) AS order_month,
        COALESCE(t.product_category_name_english, p.product_category_name) AS product_category_name,
        pay.payment_value
    FROM {{ ref('stg_orders') }} o
    JOIN {{ ref('stg_order_items') }} oi USING (order_id)
    JOIN {{ ref('stg_products') }} p USING (product_id)
    LEFT JOIN {{ ref('stg_category_translation') }} t ON p.product_category_name = t.product_category_name
    LEFT JOIN {{ ref('stg_payments') }} pay USING (order_id)
)

, category_sales AS (
    SELECT
        order_month,
        product_category_name,
        ROUND(SUM(payment_value), 2) AS total_sales
    FROM orders
    GROUP BY order_month, product_category_name
)

, ranked_categories AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY order_month
            ORDER BY total_sales DESC
        ) AS category_rank
    FROM category_sales
)

SELECT
    order_month,
    product_category_name,
    total_sales,
    category_rank
FROM ranked_categories
WHERE category_rank <= 3
ORDER BY order_month, category_rank