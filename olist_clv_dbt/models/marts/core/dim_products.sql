WITH products AS (
    SELECT
        p.product_id,
        t.product_category_name_english AS product_category,
        p.product_name_lenght,
        p.product_description_lenght,
        p.product_photos_qty,
        p.product_weight_g,
        p.product_length_cm,
        p.product_height_cm,
        p.product_width_cm
    FROM {{ ref('stg_products') }} p
    LEFT JOIN {{ ref('stg_category_translation') }} t
        ON p.product_category_name = t.product_category_name
)

SELECT * FROM products