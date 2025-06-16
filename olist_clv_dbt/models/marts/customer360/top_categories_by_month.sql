WITH category_sales AS (
    SELECT
        DATE_TRUNC(o.order_purchase_timestamp, MONTH) AS order_month,
        pt.product_category_name_english AS product_category,
        SUM(oi.price) AS total_revenue,
        COUNT(oi.product_id) AS total_items
    FROM {{ ref('stg_orders') }} o
    JOIN {{ ref('stg_order_items') }} oi
        ON o.order_id = oi.order_id
    JOIN {{ ref('stg_products') }} p
        ON oi.product_id = p.product_id
    LEFT JOIN {{ ref('stg_category_translation') }} pt
        ON p.product_category_name = pt.product_category_name
    GROUP BY order_month, product_category
)

SELECT
    order_month,
    product_category,
    total_revenue,
    total_items,
    RANK() OVER (PARTITION BY order_month ORDER BY total_revenue DESC) AS category_rank
FROM category_sales