SELECT
    oi.order_id,
    o.customer_id,
    oi.product_id,
    oi.seller_id,
    o.order_purchase_timestamp,
    oi.price,
    oi.freight_value,
    (oi.price + oi.freight_value) AS total_sale_amount
FROM {{ ref('stg_order_items') }} oi
LEFT JOIN {{ ref('stg_orders') }} o
    ON oi.order_id = o.order_id