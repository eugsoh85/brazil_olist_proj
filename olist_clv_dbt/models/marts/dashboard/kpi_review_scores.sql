{{ config(
    materialized='view'
) }}

WITH order_review_scores AS (
    SELECT
        o.order_id,
        DATE_TRUNC(DATE(o.order_purchase_timestamp), MONTH) AS order_month,
        AVG(r.review_score) AS order_avg_review_score
    FROM {{ ref('stg_orders') }} o
    JOIN {{ ref('stg_reviews') }} r USING (order_id)
    WHERE r.review_score IS NOT NULL
      AND o.order_status IN ('delivered', 'shipped')
    GROUP BY o.order_id, order_month
)

SELECT
    order_month,
    ROUND(AVG(order_avg_review_score), 2) AS avg_review_score,
    COUNT(*) AS total_orders_reviewed
FROM order_review_scores
GROUP BY order_month
ORDER BY order_month