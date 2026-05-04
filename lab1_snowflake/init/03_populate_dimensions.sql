
INSERT INTO dim_country (country_name)
SELECT DISTINCT country_name
FROM (
    SELECT blank_to_null(customer_country) AS country_name FROM mock_data
    UNION
    SELECT blank_to_null(seller_country) FROM mock_data
    UNION
    SELECT blank_to_null(store_country) FROM mock_data
    UNION
    SELECT blank_to_null(supplier_country) FROM mock_data
) countries
WHERE country_name IS NOT NULL
ON CONFLICT (country_name) DO NOTHING;

INSERT INTO dim_city (city_name, country_key)
SELECT DISTINCT city_name, c.country_key
FROM (
    SELECT blank_to_null(store_city) AS city_name, blank_to_null(store_country) AS country_name FROM mock_data
    UNION
    SELECT blank_to_null(supplier_city), blank_to_null(supplier_country) FROM mock_data
) cities
JOIN dim_country c ON c.country_name = cities.country_name
WHERE city_name IS NOT NULL
ON CONFLICT (city_name, country_key) DO NOTHING;

INSERT INTO dim_pet_type (pet_type_name)
SELECT DISTINCT blank_to_null(customer_pet_type)
FROM mock_data
WHERE blank_to_null(customer_pet_type) IS NOT NULL
ON CONFLICT (pet_type_name) DO NOTHING;

INSERT INTO dim_product_category (product_category_name)
SELECT DISTINCT blank_to_null(product_category)
FROM mock_data
WHERE blank_to_null(product_category) IS NOT NULL
ON CONFLICT (product_category_name) DO NOTHING;

INSERT INTO dim_pet_category (pet_category_name)
SELECT DISTINCT blank_to_null(pet_category)
FROM mock_data
WHERE blank_to_null(pet_category) IS NOT NULL
ON CONFLICT (pet_category_name) DO NOTHING;

INSERT INTO dim_brand (brand_name)
SELECT DISTINCT blank_to_null(product_brand)
FROM mock_data
WHERE blank_to_null(product_brand) IS NOT NULL
ON CONFLICT (brand_name) DO NOTHING;

WITH prepared AS (
    SELECT
        source_row_id,
        md5(concat_ws('|',
            key_part(supplier_name),
            key_part(supplier_contact),
            key_part(supplier_email),
            key_part(supplier_phone),
            key_part(supplier_address),
            key_part(supplier_city),
            key_part(supplier_country)
        )) AS supplier_natural_key,
        blank_to_null(supplier_name) AS supplier_name,
        blank_to_null(supplier_contact) AS supplier_contact,
        blank_to_null(supplier_email) AS supplier_email,
        blank_to_null(supplier_phone) AS supplier_phone,
        blank_to_null(supplier_address) AS supplier_address,
        blank_to_null(supplier_city) AS supplier_city,
        blank_to_null(supplier_country) AS supplier_country
    FROM mock_data
    WHERE blank_to_null(supplier_name) IS NOT NULL
), dedup AS (
    SELECT DISTINCT ON (supplier_natural_key) *
    FROM prepared
    ORDER BY supplier_natural_key, source_row_id
)
INSERT INTO dim_supplier (
    supplier_natural_key, supplier_name, supplier_contact, supplier_email,
    supplier_phone, supplier_address, supplier_city_key, supplier_country_key
)
SELECT
    d.supplier_natural_key,
    d.supplier_name,
    d.supplier_contact,
    d.supplier_email,
    d.supplier_phone,
    d.supplier_address,
    ci.city_key,
    co.country_key
FROM dedup d
LEFT JOIN dim_country co ON co.country_name = d.supplier_country
LEFT JOIN dim_city ci ON ci.city_name = d.supplier_city AND ci.country_key = co.country_key
ON CONFLICT (supplier_natural_key) DO NOTHING;

WITH prepared AS (
    SELECT
        source_row_id,
        md5(concat_ws('|',
            key_part(store_name),
            key_part(store_location),
            key_part(store_city),
            key_part(store_state),
            key_part(store_country),
            key_part(store_phone),
            key_part(store_email)
        )) AS store_natural_key,
        blank_to_null(store_name) AS store_name,
        blank_to_null(store_location) AS store_location,
        blank_to_null(store_city) AS store_city,
        blank_to_null(store_state) AS store_state,
        blank_to_null(store_country) AS store_country,
        blank_to_null(store_phone) AS store_phone,
        blank_to_null(store_email) AS store_email
    FROM mock_data
    WHERE blank_to_null(store_name) IS NOT NULL
), dedup AS (
    SELECT DISTINCT ON (store_natural_key) *
    FROM prepared
    ORDER BY store_natural_key, source_row_id
)
INSERT INTO dim_store (
    store_natural_key, store_name, store_location, store_city_key,
    store_state, store_country_key, store_phone, store_email
)
SELECT
    d.store_natural_key,
    d.store_name,
    d.store_location,
    ci.city_key,
    d.store_state,
    co.country_key,
    d.store_phone,
    d.store_email
FROM dedup d
LEFT JOIN dim_country co ON co.country_name = d.store_country
LEFT JOIN dim_city ci ON ci.city_name = d.store_city AND ci.country_key = co.country_key
ON CONFLICT (store_natural_key) DO NOTHING;

WITH prepared AS (
    SELECT
        source_row_id,
        md5(concat_ws('|', key_part(sale_customer_id), key_part(customer_email))) AS customer_natural_key,
        to_int(sale_customer_id) AS source_customer_id,
        blank_to_null(customer_first_name) AS customer_first_name,
        blank_to_null(customer_last_name) AS customer_last_name,
        blank_to_null(customer_email) AS customer_email,
        to_int(customer_age) AS customer_age,
        blank_to_null(customer_country) AS customer_country,
        blank_to_null(customer_postal_code) AS customer_postal_code
    FROM mock_data
    WHERE blank_to_null(customer_email) IS NOT NULL
), dedup AS (
    SELECT DISTINCT ON (customer_natural_key) *
    FROM prepared
    ORDER BY customer_natural_key, source_row_id
)
INSERT INTO dim_customer (
    customer_natural_key, source_customer_id, customer_first_name, customer_last_name,
    customer_email, customer_age, customer_country_key, customer_postal_code
)
SELECT
    d.customer_natural_key,
    d.source_customer_id,
    d.customer_first_name,
    d.customer_last_name,
    d.customer_email,
    d.customer_age,
    co.country_key,
    d.customer_postal_code
FROM dedup d
LEFT JOIN dim_country co ON co.country_name = d.customer_country
ON CONFLICT (customer_natural_key) DO NOTHING;

WITH prepared AS (
    SELECT
        m.source_row_id,
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
        to_int(m.sale_seller_id) AS source_seller_id,
        blank_to_null(m.seller_first_name) AS seller_first_name,
        blank_to_null(m.seller_last_name) AS seller_last_name,
        blank_to_null(m.seller_email) AS seller_email,
        blank_to_null(m.seller_country) AS seller_country,
        blank_to_null(m.seller_postal_code) AS seller_postal_code
    FROM mock_data m
    WHERE blank_to_null(m.seller_email) IS NOT NULL
), dedup AS (
    SELECT DISTINCT ON (seller_natural_key) *
    FROM prepared
    ORDER BY seller_natural_key, source_row_id
)
INSERT INTO dim_seller (
    seller_natural_key, source_seller_id, seller_first_name, seller_last_name,
    seller_email, seller_country_key, seller_postal_code, store_key
)
SELECT
    d.seller_natural_key,
    d.source_seller_id,
    d.seller_first_name,
    d.seller_last_name,
    d.seller_email,
    co.country_key,
    d.seller_postal_code,
    st.store_key
FROM dedup d
LEFT JOIN dim_country co ON co.country_name = d.seller_country
LEFT JOIN dim_store st ON st.store_natural_key = d.store_natural_key
ON CONFLICT (seller_natural_key) DO NOTHING;

WITH prepared AS (
    SELECT
        m.source_row_id,
        md5(concat_ws('|',
            key_part(m.supplier_name),
            key_part(m.supplier_contact),
            key_part(m.supplier_email),
            key_part(m.supplier_phone),
            key_part(m.supplier_address),
            key_part(m.supplier_city),
            key_part(m.supplier_country)
        )) AS supplier_natural_key,
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
        to_int(m.sale_product_id) AS source_product_id,
        blank_to_null(m.product_name) AS product_name,
        blank_to_null(m.product_category) AS product_category,
        blank_to_null(m.pet_category) AS pet_category,
        blank_to_null(m.product_brand) AS product_brand,
        to_num(m.product_price) AS product_price,
        to_int(m.product_quantity) AS product_quantity,
        to_num(m.product_weight) AS product_weight,
        blank_to_null(m.product_color) AS product_color,
        blank_to_null(m.product_size) AS product_size,
        blank_to_null(m.product_material) AS product_material,
        blank_to_null(m.product_description) AS product_description,
        to_num(m.product_rating) AS product_rating,
        to_int(m.product_reviews) AS product_reviews,
        to_mdy_date(m.product_release_date) AS product_release_date,
        to_mdy_date(m.product_expiry_date) AS product_expiry_date
    FROM mock_data m
    WHERE blank_to_null(m.product_name) IS NOT NULL
), dedup AS (
    SELECT DISTINCT ON (product_natural_key) *
    FROM prepared
    ORDER BY product_natural_key, source_row_id
)
INSERT INTO dim_product (
    product_natural_key, source_product_id, product_name, product_category_key,
    pet_category_key, brand_key, supplier_key, product_price, product_quantity,
    product_weight, product_color, product_size, product_material, product_description,
    product_rating, product_reviews, product_release_date, product_expiry_date
)
SELECT
    d.product_natural_key,
    d.source_product_id,
    d.product_name,
    pc.product_category_key,
    petc.pet_category_key,
    b.brand_key,
    sup.supplier_key,
    d.product_price,
    d.product_quantity,
    d.product_weight,
    d.product_color,
    d.product_size,
    d.product_material,
    d.product_description,
    d.product_rating,
    d.product_reviews,
    d.product_release_date,
    d.product_expiry_date
FROM dedup d
LEFT JOIN dim_product_category pc ON pc.product_category_name = d.product_category
LEFT JOIN dim_pet_category petc ON petc.pet_category_name = d.pet_category
LEFT JOIN dim_brand b ON b.brand_name = d.product_brand
LEFT JOIN dim_supplier sup ON sup.supplier_natural_key = d.supplier_natural_key
ON CONFLICT (product_natural_key) DO NOTHING;

WITH prepared AS (
    SELECT
        m.source_row_id,
        md5(concat_ws('|', key_part(m.sale_customer_id), key_part(m.customer_email))) AS customer_natural_key,
        md5(concat_ws('|',
            key_part(m.sale_customer_id),
            key_part(m.customer_email),
            key_part(m.customer_pet_name),
            key_part(m.customer_pet_type),
            key_part(m.customer_pet_breed)
        )) AS pet_natural_key,
        blank_to_null(m.customer_pet_name) AS pet_name,
        blank_to_null(m.customer_pet_type) AS pet_type_name,
        blank_to_null(m.customer_pet_breed) AS pet_breed
    FROM mock_data m
    WHERE blank_to_null(m.customer_pet_name) IS NOT NULL
), dedup AS (
    SELECT DISTINCT ON (pet_natural_key) *
    FROM prepared
    ORDER BY pet_natural_key, source_row_id
)
INSERT INTO dim_pet (pet_natural_key, pet_name, pet_type_key, pet_breed, customer_key)
SELECT
    d.pet_natural_key,
    d.pet_name,
    pt.pet_type_key,
    d.pet_breed,
    c.customer_key
FROM dedup d
LEFT JOIN dim_pet_type pt ON pt.pet_type_name = d.pet_type_name
LEFT JOIN dim_customer c ON c.customer_natural_key = d.customer_natural_key
ON CONFLICT (pet_natural_key) DO NOTHING;
