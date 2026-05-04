from urllib import request, parse, error
from pyspark.sql import SparkSession, functions as F

POSTGRES_URL = "jdbc:postgresql://postgres:5432/lab2_db"
POSTGRES_PROPS = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver",
}

CLICKHOUSE_HTTP = "http://clickhouse:8123/"
CLICKHOUSE_JDBC = "jdbc:clickhouse://clickhouse:8123/reports"
CLICKHOUSE_USER = "user"
CLICKHOUSE_PASSWORD = "password"
CLICKHOUSE_DRIVER = "com.clickhouse.jdbc.ClickHouseDriver"

spark = (
    SparkSession.builder
    .appName("lab2_create_clickhouse_reports")
    .config("spark.driver.extraClassPath", "/opt/spark/drivers/*")
    .config("spark.executor.extraClassPath", "/opt/spark/drivers/*")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


def ch_exec(sql: str) -> None:
    params = parse.urlencode({"user": CLICKHOUSE_USER, "password": CLICKHOUSE_PASSWORD})
    url = f"{CLICKHOUSE_HTTP}?{params}"
    req = request.Request(url, data=sql.encode("utf-8"), method="POST")
    try:
        with request.urlopen(req, timeout=60) as resp:
            resp.read()
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"ClickHouse error for SQL:\n{sql}\n{body}") from exc


def read_pg(table_name: str):
    return spark.read.jdbc(url=POSTGRES_URL, table=table_name, properties=POSTGRES_PROPS)


def s(col_name):
    return F.coalesce(F.col(col_name).cast("string"), F.lit(""))


def write_ch(df, table_name: str, create_sql: str) -> None:
    full_name = f"reports.{table_name}"
    print(f"Writing ClickHouse table {full_name}: {df.count()} rows")
    ch_exec(f"DROP TABLE IF EXISTS {full_name}")
    ch_exec(create_sql)
    (
        df.write
        .mode("append")
        .format("jdbc")
        .option("url", CLICKHOUSE_JDBC)
        .option("dbtable", full_name)
        .option("user", CLICKHOUSE_USER)
        .option("password", CLICKHOUSE_PASSWORD)
        .option("driver", CLICKHOUSE_DRIVER)
        .save()
    )


ch_exec("CREATE DATABASE IF NOT EXISTS reports")

fact = read_pg("fact_sales").select(
    "sale_key", "sale_date", "customer_key", "seller_key", "product_key", "store_key", "supplier_key", "pet_key",
    F.col("sale_quantity").cast("long").alias("sale_quantity"),
    F.col("sale_total_price").cast("double").alias("sale_total_price"),
    F.col("sale_unit_price").cast("double").alias("sale_unit_price"),
)
product = read_pg("dim_product")
supplier = read_pg("dim_supplier")
store = read_pg("dim_store")
customer = read_pg("dim_customer")

report_product_sales = (
    fact.join(product, "product_key", "left")
    .groupBy("product_key", "product_name", "product_category_name", "brand_name")
    .agg(
        F.count("sale_key").cast("long").alias("sales_count"),
        F.sum("sale_quantity").cast("long").alias("total_quantity"),
        F.round(F.sum("sale_total_price"), 2).cast("double").alias("revenue"),
        F.round(F.avg(F.col("product_rating").cast("double")), 2).cast("double").alias("avg_rating"),
        F.max(F.col("product_reviews").cast("long")).alias("product_reviews"),
    )
    .select(
        F.col("product_key").cast("int"),
        s("product_name").alias("product_name"),
        s("product_category_name").alias("product_category_name"),
        s("brand_name").alias("brand_name"),
        "sales_count", "total_quantity", "revenue", "avg_rating",
        F.coalesce(F.col("product_reviews"), F.lit(0)).cast("long").alias("product_reviews")
    )
)
write_ch(report_product_sales, "report_product_sales", """
CREATE TABLE reports.report_product_sales (
    product_key Int32,
    product_name String,
    product_category_name String,
    brand_name String,
    sales_count Int64,
    total_quantity Int64,
    revenue Float64,
    avg_rating Float64,
    product_reviews Int64
) ENGINE = MergeTree()
ORDER BY (total_quantity, revenue, product_key)
""")

report_customer_sales = (
    fact.join(customer, "customer_key", "left")
    .groupBy("customer_key", "customer_first_name", "customer_last_name", "customer_email", "customer_country")
    .agg(
        F.count("sale_key").cast("long").alias("sales_count"),
        F.sum("sale_quantity").cast("long").alias("items_bought"),
        F.round(F.sum("sale_total_price"), 2).cast("double").alias("total_spent"),
        F.round(F.avg("sale_total_price"), 2).cast("double").alias("avg_check"),
    )
    .select(
        F.col("customer_key").cast("int"),
        s("customer_first_name").alias("customer_first_name"),
        s("customer_last_name").alias("customer_last_name"),
        s("customer_email").alias("customer_email"),
        s("customer_country").alias("customer_country"),
        "sales_count", "items_bought", "total_spent", "avg_check"
    )
)
write_ch(report_customer_sales, "report_customer_sales", """
CREATE TABLE reports.report_customer_sales (
    customer_key Int32,
    customer_first_name String,
    customer_last_name String,
    customer_email String,
    customer_country String,
    sales_count Int64,
    items_bought Int64,
    total_spent Float64,
    avg_check Float64
) ENGINE = MergeTree()
ORDER BY (total_spent, customer_key)
""")

report_time_sales = (
    fact.withColumn("sale_year", F.year("sale_date").cast("int"))
    .withColumn("sale_month", F.month("sale_date").cast("int"))
    .groupBy("sale_year", "sale_month")
    .agg(
        F.count("sale_key").cast("long").alias("sales_count"),
        F.sum("sale_quantity").cast("long").alias("items_sold"),
        F.round(F.sum("sale_total_price"), 2).cast("double").alias("revenue"),
        F.round(F.avg("sale_total_price"), 2).cast("double").alias("avg_order_value"),
    )
    .select("sale_year", "sale_month", "sales_count", "items_sold", "revenue", "avg_order_value")
)
write_ch(report_time_sales, "report_time_sales", """
CREATE TABLE reports.report_time_sales (
    sale_year Int32,
    sale_month Int32,
    sales_count Int64,
    items_sold Int64,
    revenue Float64,
    avg_order_value Float64
) ENGINE = MergeTree()
ORDER BY (sale_year, sale_month)
""")

report_store_sales = (
    fact.join(store, "store_key", "left")
    .groupBy("store_key", "store_name", "store_city", "store_country")
    .agg(
        F.count("sale_key").cast("long").alias("sales_count"),
        F.sum("sale_quantity").cast("long").alias("items_sold"),
        F.round(F.sum("sale_total_price"), 2).cast("double").alias("revenue"),
        F.round(F.avg("sale_total_price"), 2).cast("double").alias("avg_check"),
    )
    .select(
        F.col("store_key").cast("int"),
        s("store_name").alias("store_name"),
        s("store_city").alias("store_city"),
        s("store_country").alias("store_country"),
        "sales_count", "items_sold", "revenue", "avg_check"
    )
)
write_ch(report_store_sales, "report_store_sales", """
CREATE TABLE reports.report_store_sales (
    store_key Int32,
    store_name String,
    store_city String,
    store_country String,
    sales_count Int64,
    items_sold Int64,
    revenue Float64,
    avg_check Float64
) ENGINE = MergeTree()
ORDER BY (revenue, store_key)
""")

report_supplier_sales = (
    fact.join(supplier, "supplier_key", "left")
    .join(product.select("product_key", "product_price"), "product_key", "left")
    .groupBy("supplier_key", "supplier_name", "supplier_country")
    .agg(
        F.count("sale_key").cast("long").alias("sales_count"),
        F.sum("sale_quantity").cast("long").alias("items_sold"),
        F.round(F.sum("sale_total_price"), 2).cast("double").alias("revenue"),
        F.round(F.avg(F.col("product_price").cast("double")), 2).cast("double").alias("avg_product_price"),
    )
    .select(
        F.col("supplier_key").cast("int"),
        s("supplier_name").alias("supplier_name"),
        s("supplier_country").alias("supplier_country"),
        "sales_count", "items_sold", "revenue", "avg_product_price"
    )
)
write_ch(report_supplier_sales, "report_supplier_sales", """
CREATE TABLE reports.report_supplier_sales (
    supplier_key Int32,
    supplier_name String,
    supplier_country String,
    sales_count Int64,
    items_sold Int64,
    revenue Float64,
    avg_product_price Float64
) ENGINE = MergeTree()
ORDER BY (revenue, supplier_key)
""")

product_sales = fact.groupBy("product_key").agg(
    F.count("sale_key").cast("long").alias("sales_count"),
    F.sum("sale_quantity").cast("long").alias("items_sold"),
    F.round(F.sum("sale_total_price"), 2).cast("double").alias("revenue"),
)
report_product_quality = (
    product.join(product_sales, "product_key", "left")
    .select(
        F.col("product_key").cast("int").alias("product_key"),
        s("product_name").alias("product_name"),
        s("product_category_name").alias("product_category_name"),
        F.col("product_rating").cast("double").alias("avg_rating"),
        F.col("product_reviews").cast("long").alias("total_reviews"),
        F.coalesce(F.col("sales_count"), F.lit(0)).cast("long").alias("sales_count"),
        F.coalesce(F.col("items_sold"), F.lit(0)).cast("long").alias("items_sold"),
        F.coalesce(F.col("revenue"), F.lit(0.0)).cast("double").alias("revenue"),
    )
)
try:
    corr = report_product_quality.select("avg_rating", "items_sold").stat.corr("avg_rating", "items_sold")
except Exception:
    corr = None
if corr is None or corr != corr:
    corr = 0.0
report_product_quality = report_product_quality.withColumn("rating_sales_correlation", F.lit(float(corr)).cast("double"))
write_ch(report_product_quality, "report_product_quality", """
CREATE TABLE reports.report_product_quality (
    product_key Int32,
    product_name String,
    product_category_name String,
    avg_rating Float64,
    total_reviews Int64,
    sales_count Int64,
    items_sold Int64,
    revenue Float64,
    rating_sales_correlation Float64
) ENGINE = MergeTree()
ORDER BY (avg_rating, product_key)
""")

print("All 6 ClickHouse reports are ready.")
spark.stop()
