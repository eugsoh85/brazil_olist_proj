WITH reviews_with_category AS (
    SELECT
        DATE_TRUNC(o.order_purchase_timestamp, MONTH) AS order_month,
        pt.product_category_name_english AS product_category,
        r.review_score
    FROM {{ ref('stg_orders') }} o
    JOIN {{ ref('stg_reviews') }} r
        ON o.order_id = r.order_id
    JOIN {{ ref('stg_order_items') }} oi
        ON o.order_id = oi.order_id
    JOIN {{ ref('stg_products') }} p
        ON oi.product_id = p.product_id
    LEFT JOIN {{ ref('stg_category_translation') }} pt
        ON p.product_category_name = pt.product_category_name
)

SELECT
    order_month,
    product_category,
    COUNT(*) AS review_count,
    AVG(review_score) AS avg_review_score
FROM reviews_with_category
GROUP BY order_month, product_category