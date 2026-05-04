
WITH prepared AS (
    SELECT
        m.source_row_id,
        to_int(m.id) AS source_sale_id,
        to_mdy_date(m.sale_date) AS sale_date,
        md5(concat_ws('|', key_part(m.sale_customer_id), key_part(m.customer_email))) AS customer_natural_key,
        md5(concat_ws('|',
            key_part(m.store_name),
            key_part(m.store_location),
            key_part(m.store_city),
            key_part(m.store_state),
            key_part(m.store_country),
            key_part(m.store_phone),
            key_part(m.store_email)
        )) AS store_natural_key,
        md5(concat_ws('|',
            key_part(m.sale_seller_id),
            key_part(m.seller_email),
            key_part(m.store_name),
            key_part(m.store_location),
            key_part(m.store_city),
            key_part(m.store_country)
        )) AS seller_natural_key,
        md5(concat_ws('|',
            key_part(m.sale_product_id),
            key_part(m.product_name),
            key_part(m.product_category),
            key_part(m.pet_category),
            key_part(m.product_price),
            key_part(m.product_quantity),
            key_part(m.product_weight),
            key_part(m.product_color),
            key_part(m.product_size),
            key_part(m.product_brand),
            key_part(m.product_material),
            key_part(m.product_description),
            key_part(m.product_release_date),
            key_part(m.product_expiry_date),
            key_part(m.supplier_name),
            key_part(m.supplier_email)
        )) AS product_natural_key,
        md5(concat_ws('|',
            key_part(m.sale_customer_id),
            key_part(m.customer_email),
            key_part(m.customer_pet_name),
            key_part(m.customer_pet_type),
            key_part(m.customer_pet_breed)
        )) AS pet_natural_key,
        to_int(m.sale_quantity) AS sale_quantity,
        to_num(m.sale_total_price) AS sale_total_price
    FROM mock_data m
)
INSERT INTO fact_sales (
    source_row_id, source_sale_id, sale_date, customer_key, seller_key,
    product_key, store_key, pet_key, sale_quantity, sale_total_price, sale_unit_price
)
SELECT
    p.source_row_id,
    p.source_sale_id,
    p.sale_date,
    c.customer_key,
    sel.seller_key,
    prod.product_key,
    st.store_key,
    pet.pet_key,
    p.sale_quantity,
    p.sale_total_price,
    ROUND(p.sale_total_price / NULLIF(p.sale_quantity, 0), 2) AS sale_unit_price
FROM prepared p
LEFT JOIN dim_customer c ON c.customer_natural_key = p.customer_natural_key
LEFT JOIN dim_seller sel ON sel.seller_natural_key = p.seller_natural_key
LEFT JOIN dim_product prod ON prod.product_natural_key = p.product_natural_key
LEFT JOIN dim_store st ON st.store_natural_key = p.store_natural_key
LEFT JOIN dim_pet pet ON pet.pet_natural_key = p.pet_natural_key
ON CONFLICT (source_row_id) DO NOTHING;
