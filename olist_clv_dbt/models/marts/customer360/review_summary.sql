WITH review_status AS (
    SELECT
        o.customer_id,
        DATE_TRUNC(o.order_purchase_timestamp, MONTH) AS order_month,
        r.review_score
    FROM {{ ref('stg_orders') }} o
    JOIN {{ ref('stg_reviews') }} r
      ON o.order_id = r.order_id
),

monthly_review_summary AS (
    SELECT
        order_month,
        COUNT(DISTINCT customer_id) AS total_customers,
        COUNT(DISTINCT CASE WHEN review_score >= 3 THEN customer_id END) AS satisfied_customers
    FROM review_status
    GROUP BY order_month
)

SELECT
    order_month,
    total_customers,
    satisfied_customers,
    SAFE_DIVIDE(satisfied_customers, total_customers) AS pct_satisfied_customers
FROM monthly_review_summary
