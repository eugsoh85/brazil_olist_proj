WITH customer_sales AS (
    SELECT
        o.customer_id,
        DATE_TRUNC(o.order_purchase_timestamp, MONTH) AS order_month,
        SUM(oi.price + oi.freight_value) AS total_sales
    FROM {{ ref('stg_orders') }} o
    JOIN {{ ref('stg_order_items') }} oi
      ON o.order_id = oi.order_id
    GROUP BY o.customer_id, order_month
),

customer_lifetime_value AS (
    SELECT
        customer_id,
        COUNT(DISTINCT order_month) AS active_months,
        SUM(total_sales) AS clv_total,
        AVG(total_sales) AS avg_monthly_spend
    FROM customer_sales
    GROUP BY customer_id
)

SELECT
    clv.customer_id,
    d.customer_state,
    d.customer_city,
    clv.clv_total,
    clv.avg_monthly_spend,
    clv.active_months
FROM customer_lifetime_value clv
LEFT JOIN {{ ref('dim_customers') }} d
  ON clv.customer_id = d.customer_id