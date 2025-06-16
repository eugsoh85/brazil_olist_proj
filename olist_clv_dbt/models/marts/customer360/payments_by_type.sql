SELECT
    DATE_TRUNC(o.order_purchase_timestamp, MONTH) AS order_month,
    p.payment_type,
    COUNT(p.order_id) AS payment_count,
    SUM(p.payment_value) AS total_paid
FROM {{ ref('stg_payments') }} p
JOIN {{ ref('stg_orders') }} o
    ON p.order_id = o.order_id
GROUP BY order_month, payment_type