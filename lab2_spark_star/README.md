# Лабораторная работа №2: Spark ETL + PostgreSQL + ClickHouse

Проект реализует ETL-пайплайн для лабораторной работы №2.

1. PostgreSQL загружает 10 CSV-файлов в сырую таблицу `mock_data`.
2. Spark читает `mock_data` из PostgreSQL и строит модель **«звезда»** в PostgreSQL.
3. Spark читает построенную модель и создаёт 6 аналитических витрин в ClickHouse.

## Состав проекта

```text
lab2_spark/
├── docker-compose.yml
├── download_data.ps1 / download_data.sh
├── download_jars.ps1 / download_jars.sh
├── init/
│   └── 01_load_raw.sql
├── scripts/
│   ├── 01_build_star.py
│   └── 02_create_clickhouse_reports.py
├── sql/
│   └── checks.sql
└── report.md
```

## Модель данных

В PostgreSQL строится схема **«звезда»**:

```text
                 dim_customer
                      |
    dim_seller -- fact_sales -- dim_product
                      |
      dim_store -- fact_sales -- dim_supplier
                      |
                    dim_pet
```

Таблицы измерений не ссылаются друг на друга. Все ключи измерений хранятся напрямую в `fact_sales`:

```text
customer_key
seller_key
product_key
store_key
supplier_key
pet_key
```

Категория товара, категория питомца и бренд хранятся как атрибуты `dim_product`. Город и страна магазина хранятся как атрибуты `dim_store`. Город и страна поставщика хранятся как атрибуты `dim_supplier`.

## 1. Подготовка на Windows

Скачай CSV-файлы:

```powershell
powershell -ExecutionPolicy Bypass -File .\download_data.ps1
```

Скачай JDBC-драйверы:

```powershell
powershell -ExecutionPolicy Bypass -File .\download_jars.ps1
```

В папке `data` должны быть файлы:

```text
MOCK_DATA.csv
MOCK_DATA (1).csv
...
MOCK_DATA (9).csv
```

В папке `jars` должны быть файлы:

```text
postgresql-42.7.4.jar
clickhouse-jdbc-0.6.0-all.jar
```

## 2. Запуск контейнеров

```cmd
docker compose up -d
```

Проверка загрузки CSV в PostgreSQL:

```cmd
docker exec -it lab2_postgres psql -U postgres -d lab2_db -c "SELECT COUNT(*) FROM mock_data;"
```

Ожидаемый результат:

```text
10000
```

## 3. Построение звезды в PostgreSQL

Команда для Windows `cmd`:

```cmd
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --jars /opt/spark/drivers/postgresql-42.7.4.jar --driver-class-path /opt/spark/drivers/postgresql-42.7.4.jar --conf spark.driver.extraClassPath=/opt/spark/drivers/postgresql-42.7.4.jar --conf spark.executor.extraClassPath=/opt/spark/drivers/postgresql-42.7.4.jar /opt/spark/app/01_build_star.py
```

Проверка результата:

```cmd
docker exec -it lab2_postgres psql -U postgres -d lab2_db -c "SELECT * FROM view_fact_quality;"
```

Ожидаемо: `raw_rows = 10000`, `fact_rows = 10000`, `not_loaded_rows = 0`.

Посмотреть сводку таблиц:

```cmd
docker exec -it lab2_postgres psql -U postgres -d lab2_db -c "SELECT * FROM view_schema_summary ORDER BY table_name;"
```

В сводке должны быть:

```text
dim_customer
dim_seller
dim_product
dim_store
dim_supplier
dim_pet
fact_sales
mock_data
```

## 4. Создание отчётов в ClickHouse

```cmd
docker compose exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --jars /opt/spark/drivers/postgresql-42.7.4.jar,/opt/spark/drivers/clickhouse-jdbc-0.6.0-all.jar --driver-class-path /opt/spark/drivers/postgresql-42.7.4.jar:/opt/spark/drivers/clickhouse-jdbc-0.6.0-all.jar --conf spark.driver.extraClassPath=/opt/spark/drivers/postgresql-42.7.4.jar:/opt/spark/drivers/clickhouse-jdbc-0.6.0-all.jar --conf spark.executor.extraClassPath=/opt/spark/drivers/postgresql-42.7.4.jar:/opt/spark/drivers/clickhouse-jdbc-0.6.0-all.jar /opt/spark/app/02_create_clickhouse_reports.py
```

Будут созданы 6 таблиц в базе `reports`:

```text
reports.report_product_sales
reports.report_customer_sales
reports.report_time_sales
reports.report_store_sales
reports.report_supplier_sales
reports.report_product_quality
```

## 5. Проверка отчётов в ClickHouse

Топ-10 товаров:

```cmd
docker exec -it lab2_clickhouse clickhouse-client --user user --password password --query "SELECT product_name, product_category_name, total_quantity, revenue FROM reports.report_product_sales ORDER BY total_quantity DESC LIMIT 10 FORMAT Pretty;"
```

Топ-10 клиентов:

```cmd
docker exec -it lab2_clickhouse clickhouse-client --user user --password password --query "SELECT customer_email, total_spent, avg_check FROM reports.report_customer_sales ORDER BY total_spent DESC LIMIT 10 FORMAT Pretty;"
```

Продажи по месяцам:

```cmd
docker exec -it lab2_clickhouse clickhouse-client --user user --password password --query "SELECT sale_year, sale_month, revenue, avg_order_value FROM reports.report_time_sales ORDER BY sale_year, sale_month FORMAT Pretty;"
```

Топ-5 магазинов:

```cmd
docker exec -it lab2_clickhouse clickhouse-client --user user --password password --query "SELECT store_name, store_city, store_country, revenue, avg_check FROM reports.report_store_sales ORDER BY revenue DESC LIMIT 5 FORMAT Pretty;"
```

Топ-5 поставщиков:

```cmd
docker exec -it lab2_clickhouse clickhouse-client --user user --password password --query "SELECT supplier_name, supplier_country, revenue, avg_product_price FROM reports.report_supplier_sales ORDER BY revenue DESC LIMIT 5 FORMAT Pretty;"
```

Качество товаров:

```cmd
docker exec -it lab2_clickhouse clickhouse-client --user user --password password --query "SELECT product_name, product_category_name, avg_rating, total_reviews, rating_sales_correlation FROM reports.report_product_quality ORDER BY avg_rating DESC LIMIT 10 FORMAT Pretty;"
```

## 6. Перезапуск с нуля

```cmd
docker compose down -v
docker compose up -d
```

После этого заново выполнить пункты 3 и 4.
