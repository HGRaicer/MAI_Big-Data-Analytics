
CREATE OR REPLACE VIEW view_schema_summary AS
SELECT 'mock_data' AS table_name, COUNT(*)::BIGINT AS row_count FROM mock_data
UNION ALL SELECT 'dim_country', COUNT(*) FROM dim_country
UNION ALL SELECT 'dim_city', COUNT(*) FROM dim_city
UNION ALL SELECT 'dim_pet_type', COUNT(*) FROM dim_pet_type
UNION ALL SELECT 'dim_product_category', COUNT(*) FROM dim_product_category
UNION ALL SELECT 'dim_pet_category', COUNT(*) FROM dim_pet_category
UNION ALL SELECT 'dim_brand', COUNT(*) FROM dim_brand
UNION ALL SELECT 'dim_supplier', COUNT(*) FROM dim_supplier
UNION ALL SELECT 'dim_store', COUNT(*) FROM dim_store
UNION ALL SELECT 'dim_customer', COUNT(*) FROM dim_customer
UNION ALL SELECT 'dim_seller', COUNT(*) FROM dim_seller
UNION ALL SELECT 'dim_product', COUNT(*) FROM dim_product
UNION ALL SELECT 'dim_pet', COUNT(*) FROM dim_pet
UNION ALL SELECT 'fact_sales', COUNT(*) FROM fact_sales
ORDER BY table_name;

CREATE OR REPLACE VIEW view_fact_quality AS
SELECT
    (SELECT COUNT(*) FROM mock_data) AS raw_rows,
    (SELECT COUNT(*) FROM fact_sales) AS fact_rows,
    (SELECT COUNT(*) FROM mock_data) - (SELECT COUNT(*) FROM fact_sales) AS not_loaded_rows,
    COUNT(*) FILTER (WHERE customer_key IS NULL) AS rows_without_customer,
    COUNT(*) FILTER (WHERE seller_key IS NULL) AS rows_without_seller,
    COUNT(*) FILTER (WHERE product_key IS NULL) AS rows_without_product,
    COUNT(*) FILTER (WHERE store_key IS NULL) AS rows_without_store,
    COUNT(*) FILTER (WHERE pet_key IS NULL) AS rows_without_pet
FROM fact_sales;

CREATE OR REPLACE VIEW view_revenue_by_store_country AS
SELECT
    co.country_name,
    COUNT(*) AS sales_count,
    SUM(f.sale_quantity) AS items_sold,
    ROUND(SUM(f.sale_total_price), 2) AS revenue
FROM fact_sales f
JOIN dim_store st ON st.store_key = f.store_key
JOIN dim_country co ON co.country_key = st.store_country_key
GROUP BY co.country_name
ORDER BY revenue DESC;

CREATE OR REPLACE VIEW view_revenue_by_brand AS
SELECT
    b.brand_name,
    COUNT(*) AS sales_count,
    SUM(f.sale_quantity) AS items_sold,
    ROUND(SUM(f.sale_total_price), 2) AS revenue
FROM fact_sales f
JOIN dim_product p ON p.product_key = f.product_key
JOIN dim_brand b ON b.brand_key = p.brand_key
GROUP BY b.brand_name
ORDER BY revenue DESC;

CREATE OR REPLACE VIEW view_revenue_by_product_category AS
SELECT
    pc.product_category_name,
    petc.pet_category_name,
    COUNT(*) AS sales_count,
    SUM(f.sale_quantity) AS items_sold,
    ROUND(SUM(f.sale_total_price), 2) AS revenue
FROM fact_sales f
JOIN dim_product p ON p.product_key = f.product_key
LEFT JOIN dim_product_category pc ON pc.product_category_key = p.product_category_key
LEFT JOIN dim_pet_category petc ON petc.pet_category_key = p.pet_category_key
GROUP BY pc.product_category_name, petc.pet_category_name
ORDER BY revenue DESC;

CREATE OR REPLACE VIEW view_top_customers AS
SELECT
    c.customer_email,
    c.customer_first_name,
    c.customer_last_name,
    co.country_name AS customer_country,
    COUNT(*) AS sales_count,
    SUM(f.sale_quantity) AS items_bought,
    ROUND(SUM(f.sale_total_price), 2) AS revenue
FROM fact_sales f
JOIN dim_customer c ON c.customer_key = f.customer_key
LEFT JOIN dim_country co ON co.country_key = c.customer_country_key
GROUP BY c.customer_email, c.customer_first_name, c.customer_last_name, co.country_name
ORDER BY revenue DESC;

CREATE OR REPLACE VIEW view_revenue_by_pet_type AS
SELECT
    pt.pet_type_name,
    COUNT(*) AS sales_count,
    SUM(f.sale_quantity) AS items_sold,
    ROUND(SUM(f.sale_total_price), 2) AS revenue
FROM fact_sales f
JOIN dim_pet pet ON pet.pet_key = f.pet_key
JOIN dim_pet_type pt ON pt.pet_type_key = pet.pet_type_key
GROUP BY pt.pet_type_name
ORDER BY revenue DESC;
