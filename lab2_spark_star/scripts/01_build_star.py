from functools import reduce
from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql.types import DecimalType

POSTGRES_URL = "jdbc:postgresql://postgres:5432/lab2_db"
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "postgres"
POSTGRES_DRIVER = "org.postgresql.Driver"

spark = (
    SparkSession.builder
    .appName("lab2_build_star_in_postgres")
    .config("spark.driver.extraClassPath", "/opt/spark/drivers/*")
    .config("spark.executor.extraClassPath", "/opt/spark/drivers/*")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

pg_props = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": POSTGRES_DRIVER,
}


def execute_pg(sql_text: str) -> None:
    gateway = spark.sparkContext._gateway
    jvm = gateway.jvm
    jvm.java.lang.Class.forName(POSTGRES_DRIVER)
    conn = jvm.java.sql.DriverManager.getConnection(POSTGRES_URL, POSTGRES_USER, POSTGRES_PASSWORD)
    stmt = conn.createStatement()
    try:
        for statement in sql_text.split(";"):
            statement = statement.strip()
            if statement:
                stmt.execute(statement)
    finally:
        stmt.close()
        conn.close()


def blank(col_name: str):
    value = F.trim(F.col(col_name).cast("string"))
    return F.when(value.isNull() | (value == ""), F.lit(None)).otherwise(value)


def key_part(col_name: str):
    return F.coalesce(F.lower(blank(col_name)), F.lit("<null>"))


def md5_key(cols):
    return F.md5(F.concat_ws("|", *[key_part(c) for c in cols]))


def to_int(col_name: str):
    return blank(col_name).cast("int")


def to_money(col_name: str):
    return blank(col_name).cast(DecimalType(18, 2))


def to_num(col_name: str):
    return blank(col_name).cast(DecimalType(18, 4))


def to_date_mdy(col_name: str):
    return F.to_date(blank(col_name), "M/d/yyyy")


def add_key(df, key_name, order_cols):
    window = Window.orderBy(*[F.col(c).asc_nulls_last() for c in order_cols])
    return df.withColumn(key_name, F.row_number().over(window).cast("int"))


def write_table(df, name: str) -> None:
    print(f"Writing {name}: {df.count()} rows")
    (
        df.write
        .mode("overwrite")
        .option("truncate", "false")
        .jdbc(url=POSTGRES_URL, table=name, properties=pg_props)
    )


execute_pg("""
DROP VIEW IF EXISTS view_fact_quality;
DROP VIEW IF EXISTS view_schema_summary;
DROP VIEW IF EXISTS view_sales_by_month;
DROP VIEW IF EXISTS view_revenue_by_product_category;
DROP VIEW IF EXISTS view_top_products;
DROP VIEW IF EXISTS view_top_customers;
DROP VIEW IF EXISTS view_supplier_sales;
DROP VIEW IF EXISTS view_store_sales;
DROP TABLE IF EXISTS fact_sales CASCADE;
DROP TABLE IF EXISTS dim_customer CASCADE;
DROP TABLE IF EXISTS dim_seller CASCADE;
DROP TABLE IF EXISTS dim_product CASCADE;
DROP TABLE IF EXISTS dim_store CASCADE;
DROP TABLE IF EXISTS dim_supplier CASCADE;
DROP TABLE IF EXISTS dim_pet CASCADE;
DROP TABLE IF EXISTS dim_country CASCADE;
DROP TABLE IF EXISTS dim_city CASCADE;
DROP TABLE IF EXISTS dim_pet_type CASCADE;
DROP TABLE IF EXISTS dim_product_category CASCADE;
DROP TABLE IF EXISTS dim_pet_category CASCADE;
DROP TABLE IF EXISTS dim_brand CASCADE;
""")

raw = spark.read.jdbc(url=POSTGRES_URL, table="mock_data", properties=pg_props).cache()
print(f"mock_data rows: {raw.count()}")

customer_base = raw.select(
    md5_key(["sale_customer_id", "customer_email"]).alias("customer_natural_key"),
    to_int("sale_customer_id").alias("source_customer_id"),
    blank("customer_first_name").alias("customer_first_name"),
    blank("customer_last_name").alias("customer_last_name"),
    blank("customer_email").alias("customer_email"),
    to_int("customer_age").alias("customer_age"),
    blank("customer_country").alias("customer_country"),
    blank("customer_postal_code").alias("customer_postal_code"),
).where(F.col("customer_email").isNotNull()).dropDuplicates(["customer_natural_key"])

dim_customer = add_key(customer_base, "customer_key", ["customer_natural_key"]).select(
    "customer_key", "customer_natural_key", "source_customer_id", "customer_first_name",
    "customer_last_name", "customer_email", "customer_age", "customer_country",
    "customer_postal_code"
)


seller_base = raw.select(
    md5_key(["sale_seller_id", "seller_email", "seller_first_name", "seller_last_name", "seller_country", "seller_postal_code"]).alias("seller_natural_key"),
    to_int("sale_seller_id").alias("source_seller_id"),
    blank("seller_first_name").alias("seller_first_name"),
    blank("seller_last_name").alias("seller_last_name"),
    blank("seller_email").alias("seller_email"),
    blank("seller_country").alias("seller_country"),
    blank("seller_postal_code").alias("seller_postal_code"),
).where(F.col("seller_email").isNotNull()).dropDuplicates(["seller_natural_key"])

dim_seller = add_key(seller_base, "seller_key", ["seller_natural_key"]).select(
    "seller_key", "seller_natural_key", "source_seller_id", "seller_first_name",
    "seller_last_name", "seller_email", "seller_country", "seller_postal_code"
)

store_base = raw.select(
    md5_key(["store_name", "store_location", "store_city", "store_state", "store_country", "store_phone", "store_email"]).alias("store_natural_key"),
    blank("store_name").alias("store_name"),
    blank("store_location").alias("store_location"),
    blank("store_city").alias("store_city"),
    blank("store_state").alias("store_state"),
    blank("store_country").alias("store_country"),
    blank("store_phone").alias("store_phone"),
    blank("store_email").alias("store_email"),
).where(F.col("store_name").isNotNull()).dropDuplicates(["store_natural_key"])

dim_store = add_key(store_base, "store_key", ["store_natural_key"]).select(
    "store_key", "store_natural_key", "store_name", "store_location", "store_city",
    "store_state", "store_country", "store_phone", "store_email"
)

supplier_base = raw.select(
    md5_key(["supplier_name", "supplier_contact", "supplier_email", "supplier_phone", "supplier_address", "supplier_city", "supplier_country"]).alias("supplier_natural_key"),
    blank("supplier_name").alias("supplier_name"),
    blank("supplier_contact").alias("supplier_contact"),
    blank("supplier_email").alias("supplier_email"),
    blank("supplier_phone").alias("supplier_phone"),
    blank("supplier_address").alias("supplier_address"),
    blank("supplier_city").alias("supplier_city"),
    blank("supplier_country").alias("supplier_country"),
).where(F.col("supplier_name").isNotNull()).dropDuplicates(["supplier_natural_key"])

dim_supplier = add_key(supplier_base, "supplier_key", ["supplier_natural_key"]).select(
    "supplier_key", "supplier_natural_key", "supplier_name", "supplier_contact",
    "supplier_email", "supplier_phone", "supplier_address", "supplier_city", "supplier_country"
)

product_base = raw.select(
    md5_key([
        "sale_product_id", "product_name", "product_category", "pet_category", "product_price",
        "product_quantity", "product_weight", "product_color", "product_size", "product_brand",
        "product_material", "product_description", "product_release_date", "product_expiry_date"
    ]).alias("product_natural_key"),
    to_int("sale_product_id").alias("source_product_id"),
    blank("product_name").alias("product_name"),
    blank("product_category").alias("product_category_name"),
    blank("pet_category").alias("pet_category_name"),
    blank("product_brand").alias("brand_name"),
    to_money("product_price").alias("product_price"),
    to_int("product_quantity").alias("product_quantity"),
    to_num("product_weight").alias("product_weight"),
    blank("product_color").alias("product_color"),
    blank("product_size").alias("product_size"),
    blank("product_material").alias("product_material"),
    blank("product_description").alias("product_description"),
    to_num("product_rating").alias("product_rating"),
    to_int("product_reviews").alias("product_reviews"),
    to_date_mdy("product_release_date").alias("product_release_date"),
    to_date_mdy("product_expiry_date").alias("product_expiry_date"),
).where(F.col("product_name").isNotNull()).dropDuplicates(["product_natural_key"])

dim_product = add_key(product_base, "product_key", ["product_natural_key"]).select(
    "product_key", "product_natural_key", "source_product_id", "product_name",
    "product_category_name", "pet_category_name", "brand_name", "product_price",
    "product_quantity", "product_weight", "product_color", "product_size",
    "product_material", "product_description", "product_rating", "product_reviews",
    "product_release_date", "product_expiry_date"
)

pet_base = raw.select(
    md5_key(["sale_customer_id", "customer_email", "customer_pet_name", "customer_pet_type", "customer_pet_breed"]).alias("pet_natural_key"),
    blank("customer_pet_name").alias("pet_name"),
    blank("customer_pet_type").alias("pet_type_name"),
    blank("customer_pet_breed").alias("pet_breed"),
).where(F.col("pet_name").isNotNull()).dropDuplicates(["pet_natural_key"])

dim_pet = add_key(pet_base, "pet_key", ["pet_natural_key"]).select(
    "pet_key", "pet_natural_key", "pet_name", "pet_type_name", "pet_breed"
)


fact_base = raw.select(
    F.col("source_row_id").cast("long").alias("source_row_id"),
    to_int("id").alias("source_sale_id"),
    to_date_mdy("sale_date").alias("sale_date"),
    md5_key(["sale_customer_id", "customer_email"]).alias("customer_natural_key"),
    md5_key(["sale_seller_id", "seller_email", "seller_first_name", "seller_last_name", "seller_country", "seller_postal_code"]).alias("seller_natural_key"),
    md5_key([
        "sale_product_id", "product_name", "product_category", "pet_category", "product_price",
        "product_quantity", "product_weight", "product_color", "product_size", "product_brand",
        "product_material", "product_description", "product_release_date", "product_expiry_date"
    ]).alias("product_natural_key"),
    md5_key(["store_name", "store_location", "store_city", "store_state", "store_country", "store_phone", "store_email"]).alias("store_natural_key"),
    md5_key(["supplier_name", "supplier_contact", "supplier_email", "supplier_phone", "supplier_address", "supplier_city", "supplier_country"]).alias("supplier_natural_key"),
    md5_key(["sale_customer_id", "customer_email", "customer_pet_name", "customer_pet_type", "customer_pet_breed"]).alias("pet_natural_key"),
    to_int("sale_quantity").alias("sale_quantity"),
    to_money("sale_total_price").alias("sale_total_price"),
)

fact_sales = (
    fact_base
    .join(dim_customer.select("customer_key", "customer_natural_key"), "customer_natural_key", "left")
    .join(dim_seller.select("seller_key", "seller_natural_key"), "seller_natural_key", "left")
    .join(dim_product.select("product_key", "product_natural_key"), "product_natural_key", "left")
    .join(dim_store.select("store_key", "store_natural_key"), "store_natural_key", "left")
    .join(dim_supplier.select("supplier_key", "supplier_natural_key"), "supplier_natural_key", "left")
    .join(dim_pet.select("pet_key", "pet_natural_key"), "pet_natural_key", "left")
    .withColumn(
        "sale_unit_price",
        F.when(F.col("sale_quantity") != 0, F.round(F.col("sale_total_price") / F.col("sale_quantity"), 2)).otherwise(F.lit(None)).cast(DecimalType(18, 2))
    )
)

fact_sales = add_key(fact_sales, "sale_key", ["source_row_id"]).select(
    "sale_key", "source_row_id", "source_sale_id", "sale_date", "customer_key",
    "seller_key", "product_key", "store_key", "supplier_key", "pet_key",
    "sale_quantity", "sale_total_price", "sale_unit_price"
)

for table_name, table_df in [
    ("dim_customer", dim_customer),
    ("dim_seller", dim_seller),
    ("dim_product", dim_product),
    ("dim_store", dim_store),
    ("dim_supplier", dim_supplier),
    ("dim_pet", dim_pet),
    ("fact_sales", fact_sales),
]:
    write_table(table_df, table_name)

execute_pg("""
CREATE OR REPLACE VIEW view_schema_summary AS
SELECT 'mock_data' AS table_name, COUNT(*)::BIGINT AS row_count FROM mock_data
UNION ALL SELECT 'dim_customer', COUNT(*) FROM dim_customer
UNION ALL SELECT 'dim_seller', COUNT(*) FROM dim_seller
UNION ALL SELECT 'dim_product', COUNT(*) FROM dim_product
UNION ALL SELECT 'dim_store', COUNT(*) FROM dim_store
UNION ALL SELECT 'dim_supplier', COUNT(*) FROM dim_supplier
UNION ALL SELECT 'dim_pet', COUNT(*) FROM dim_pet
UNION ALL SELECT 'fact_sales', COUNT(*) FROM fact_sales;

CREATE OR REPLACE VIEW view_fact_quality AS
SELECT
    (SELECT COUNT(*) FROM mock_data) AS raw_rows,
    (SELECT COUNT(*) FROM fact_sales) AS fact_rows,
    (SELECT COUNT(*) FROM mock_data) - (SELECT COUNT(*) FROM fact_sales) AS not_loaded_rows,
    COUNT(*) FILTER (WHERE customer_key IS NULL) AS rows_without_customer,
    COUNT(*) FILTER (WHERE seller_key IS NULL) AS rows_without_seller,
    COUNT(*) FILTER (WHERE product_key IS NULL) AS rows_without_product,
    COUNT(*) FILTER (WHERE store_key IS NULL) AS rows_without_store,
    COUNT(*) FILTER (WHERE supplier_key IS NULL) AS rows_without_supplier,
    COUNT(*) FILTER (WHERE pet_key IS NULL) AS rows_without_pet
FROM fact_sales;

CREATE OR REPLACE VIEW view_top_products AS
SELECT
    p.product_name,
    p.product_category_name,
    p.brand_name,
    SUM(f.sale_quantity) AS total_quantity,
    ROUND(SUM(f.sale_total_price), 2) AS revenue
FROM fact_sales f
JOIN dim_product p ON p.product_key = f.product_key
GROUP BY p.product_name, p.product_category_name, p.brand_name
ORDER BY total_quantity DESC;

CREATE OR REPLACE VIEW view_top_customers AS
SELECT
    c.customer_email,
    c.customer_first_name,
    c.customer_last_name,
    c.customer_country,
    SUM(f.sale_total_price) AS total_spent,
    ROUND(AVG(f.sale_total_price), 2) AS avg_check
FROM fact_sales f
JOIN dim_customer c ON c.customer_key = f.customer_key
GROUP BY c.customer_email, c.customer_first_name, c.customer_last_name, c.customer_country
ORDER BY total_spent DESC;

CREATE OR REPLACE VIEW view_sales_by_month AS
SELECT
    EXTRACT(YEAR FROM sale_date)::INT AS sale_year,
    EXTRACT(MONTH FROM sale_date)::INT AS sale_month,
    COUNT(*) AS sales_count,
    SUM(sale_total_price) AS revenue,
    ROUND(AVG(sale_total_price), 2) AS avg_order_value
FROM fact_sales
GROUP BY sale_year, sale_month
ORDER BY sale_year, sale_month;

CREATE OR REPLACE VIEW view_revenue_by_product_category AS
SELECT
    p.product_category_name,
    p.pet_category_name,
    COUNT(*) AS sales_count,
    SUM(f.sale_quantity) AS items_sold,
    ROUND(SUM(f.sale_total_price), 2) AS revenue
FROM fact_sales f
JOIN dim_product p ON p.product_key = f.product_key
GROUP BY p.product_category_name, p.pet_category_name
ORDER BY revenue DESC;

CREATE OR REPLACE VIEW view_supplier_sales AS
SELECT
    s.supplier_name,
    s.supplier_country,
    COUNT(*) AS sales_count,
    SUM(f.sale_quantity) AS items_sold,
    ROUND(SUM(f.sale_total_price), 2) AS revenue
FROM fact_sales f
JOIN dim_supplier s ON s.supplier_key = f.supplier_key
GROUP BY s.supplier_name, s.supplier_country
ORDER BY revenue DESC;

CREATE OR REPLACE VIEW view_store_sales AS
SELECT
    st.store_name,
    st.store_city,
    st.store_country,
    COUNT(*) AS sales_count,
    SUM(f.sale_quantity) AS items_sold,
    ROUND(SUM(f.sale_total_price), 2) AS revenue
FROM fact_sales f
JOIN dim_store st ON st.store_key = f.store_key
GROUP BY st.store_name, st.store_city, st.store_country
ORDER BY revenue DESC;
""")

print("Star schema and PostgreSQL check views are ready.")
spark.stop()
