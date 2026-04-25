#!/usr/bin/env python3
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("spark_create_marts").getOrCreate()
spark.sparkContext.setLogLevel("WARN")


def jdbc_read(table_or_query: str) -> DataFrame:
    return (
        spark.read.format("jdbc")
        .option("url", os.getenv("SRC_JDBC_URL"))
        .option("user", os.getenv("SRC_JDBC_USER"))
        .option("password", os.getenv("SRC_JDBC_PASSWORD"))
        .option("driver", os.getenv("SRC_JDBC_DRIVER"))
        .option("dbtable", table_or_query)
        .load()
    )


def jdbc_write(df: DataFrame, table: str) -> None:
    (
        df.write.format("jdbc")
        .option("url", os.getenv("DST_JDBC_URL"))
        .option("user", os.getenv("DST_JDBC_USER"))
        .option("password", os.getenv("DST_JDBC_PASSWORD"))
        .option("driver", os.getenv("DST_JDBC_DRIVER"))
        .option("dbtable", table)
        .mode(os.getenv("DST_WRITE_MODE", "append"))
        .save()
    )


sales = (
    jdbc_read("sales")
    .select(
        F.col("id").alias("sale_id"),
        F.col("customer_id"),
        F.col("date").alias("sale_date"),
        F.col("seller_id"),
        F.col("store_id"),
        F.col("product_id"),
        F.col("quantity").cast("long").alias("sale_quantity"),
        F.col("total_price").cast("double").alias("sale_total_price"),
    )
    .cache()
)

products = (
    jdbc_read("products")
    .select(
        F.col("id").alias("product_id"),
        F.col("name").alias("product_name"),
        F.col("price").cast("double").alias("product_price"),
        F.col("quantity").cast("long").alias("stock_quantity"),
        F.col("supplier_id"),
        F.col("category_id"),
        F.col("rating").cast("double").alias("product_rating"),
        F.col("reviews").cast("long").alias("product_reviews"),
    )
    .cache()
)

product_categories = jdbc_read("product_categories").select(
    F.col("id").alias("category_id"), F.col("name").alias("category_name")
)

customers = jdbc_read("customers").select(
    F.col("id").alias("customer_id"),
    F.col("first_name").alias("customer_first_name"),
    F.col("last_name").alias("customer_last_name"),
    F.col("email").alias("customer_email"),
    F.col("country").alias("customer_country"),
)

stores = jdbc_read("stores").select(
    F.col("id").alias("store_id"),
    F.col("name").alias("store_name"),
    F.col("city").alias("store_city"),
    F.col("country").alias("store_country"),
)

suppliers = jdbc_read("product_suppliers").select(
    F.col("id").alias("supplier_id"),
    F.col("name").alias("supplier_name"),
    F.col("city").alias("supplier_city"),
    F.col("country").alias("supplier_country"),
)

sales_products = (
    sales.alias("s")
    .join(products.alias("p"), "product_id", "inner")
    .join(product_categories.alias("c"), "category_id", "inner")
    .cache()
)

product_quantity_window = Window.orderBy(
    F.desc("total_quantity"), F.desc("total_revenue")
)
category_window = Window.partitionBy("category_id")

product_sales_mart = (
    sales_products.groupBy("product_id", "product_name", "category_id", "category_name")
    .agg(
        F.sum("sale_total_price").alias("total_revenue"),
        F.sum("sale_quantity").alias("total_quantity"),
        F.count("sale_id").alias("sales_count"),
        F.avg("product_rating").alias("avg_rating"),
        F.max("product_reviews").alias("reviews_count"),
    )
    .withColumn("category_total_revenue", F.sum("total_revenue").over(category_window))
    .withColumn("sales_rank", F.row_number().over(product_quantity_window))
    .withColumn("is_top_10_by_quantity", F.col("sales_rank") <= F.lit(10))
    .select(
        "product_id",
        "product_name",
        "category_id",
        "category_name",
        F.round("total_revenue", 2).alias("total_revenue"),
        "total_quantity",
        "sales_count",
        F.round("category_total_revenue", 2).alias("category_total_revenue"),
        F.round("avg_rating", 2).alias("avg_rating"),
        "reviews_count",
        "sales_rank",
        "is_top_10_by_quantity",
    )
)

jdbc_write(product_sales_mart, "product_sales")

customer_base = sales.alias("s").join(customers.alias("c"), "customer_id", "inner")
customer_rank_window = Window.orderBy(F.desc("total_purchase_amount"))

customer_sales_mart = (
    customer_base.groupBy(
        "customer_id",
        "customer_first_name",
        "customer_last_name",
        "customer_email",
        "customer_country",
    )
    .agg(
        F.sum("sale_total_price").alias("total_purchase_amount"),
        F.count("sale_id").alias("orders_count"),
        F.avg("sale_total_price").alias("avg_check"),
    )
    .join(
        customers.groupBy("customer_country").agg(
            F.countDistinct("customer_id").alias("country_customers_count")
        ),
        "customer_country",
        "left",
    )
    .withColumn("purchase_rank", F.row_number().over(customer_rank_window))
    .withColumn("is_top_10_customer", F.col("purchase_rank") <= F.lit(10))
    .select(
        "customer_id",
        "customer_first_name",
        "customer_last_name",
        "customer_email",
        "customer_country",
        F.round("total_purchase_amount", 2).alias("total_purchase_amount"),
        "orders_count",
        F.round("avg_check", 2).alias("avg_check"),
        "country_customers_count",
        "purchase_rank",
        "is_top_10_customer",
    )
)

jdbc_write(customer_sales_mart, "customer_sales")

monthly_sales = (
    sales.withColumn("sale_year", F.year("sale_date"))
    .withColumn("sale_month", F.month("sale_date"))
    .withColumn("period_start", F.trunc("sale_date", "month"))
    .groupBy("sale_year", "sale_month", "period_start")
    .agg(
        F.sum("sale_total_price").alias("monthly_revenue"),
        F.sum("sale_quantity").alias("monthly_quantity"),
        F.count("sale_id").alias("monthly_orders"),
        F.avg("sale_total_price").alias("monthly_avg_order"),
    )
)

yearly_sales = monthly_sales.groupBy("sale_year").agg(
    F.sum("monthly_revenue").alias("yearly_revenue"),
    F.sum("monthly_orders").alias("yearly_orders"),
)

time_window = Window.orderBy("period_start")
time_sales_mart = (
    monthly_sales.join(yearly_sales, "sale_year", "inner")
    .withColumn("previous_month_revenue", F.lag("monthly_revenue").over(time_window))
    .withColumn(
        "monthly_revenue_delta",
        F.col("monthly_revenue") - F.col("previous_month_revenue"),
    )
    .select(
        "period_start",
        "sale_year",
        "sale_month",
        F.round("monthly_revenue", 2).alias("monthly_revenue"),
        "monthly_quantity",
        "monthly_orders",
        F.round("monthly_avg_order", 2).alias("monthly_avg_order"),
        F.round("yearly_revenue", 2).alias("yearly_revenue"),
        "yearly_orders",
        F.round("previous_month_revenue", 2).alias("previous_month_revenue"),
        F.round("monthly_revenue_delta", 2).alias("monthly_revenue_delta"),
    )
)

jdbc_write(time_sales_mart, "time_sales")

store_base = sales.alias("s").join(stores.alias("st"), "store_id", "inner")
store_rank_window = Window.orderBy(F.desc("total_revenue"))

store_sales_mart = (
    store_base.groupBy("store_id", "store_name", "store_city", "store_country")
    .agg(
        F.sum("sale_total_price").alias("total_revenue"),
        F.sum("sale_quantity").alias("total_quantity"),
        F.count("sale_id").alias("orders_count"),
        F.avg("sale_total_price").alias("avg_check"),
    )
    .join(
        store_base.groupBy("store_city", "store_country").agg(
            F.sum("sale_total_price").alias("city_country_total_revenue"),
            F.count("sale_id").alias("city_country_orders_count"),
        ),
        ["store_city", "store_country"],
        "left",
    )
    .withColumn("revenue_rank", F.row_number().over(store_rank_window))
    .withColumn("is_top_5_store", F.col("revenue_rank") <= F.lit(5))
    .select(
        "store_id",
        "store_name",
        "store_city",
        "store_country",
        F.round("total_revenue", 2).alias("total_revenue"),
        "total_quantity",
        "orders_count",
        F.round("avg_check", 2).alias("avg_check"),
        F.round("city_country_total_revenue", 2).alias("city_country_total_revenue"),
        "city_country_orders_count",
        "revenue_rank",
        "is_top_5_store",
    )
)

jdbc_write(store_sales_mart, "store_sales")

supplier_base = sales_products.join(suppliers, "supplier_id", "inner")
supplier_rank_window = Window.orderBy(F.desc("total_revenue"))
supplier_product_prices = products.groupBy("supplier_id").agg(
    F.avg("product_price").alias("avg_product_price")
)

supplier_sales_mart = (
    supplier_base.groupBy(
        "supplier_id", "supplier_name", "supplier_city", "supplier_country"
    )
    .agg(
        F.sum("sale_total_price").alias("total_revenue"),
        F.sum("sale_quantity").alias("total_quantity"),
        F.count("sale_id").alias("orders_count"),
    )
    .join(supplier_product_prices, "supplier_id", "left")
    .join(
        supplier_base.groupBy("supplier_country").agg(
            F.sum("sale_total_price").alias("supplier_country_total_revenue"),
            F.count("sale_id").alias("supplier_country_orders_count"),
        ),
        "supplier_country",
        "left",
    )
    .withColumn("revenue_rank", F.row_number().over(supplier_rank_window))
    .withColumn("is_top_5_supplier", F.col("revenue_rank") <= F.lit(5))
    .select(
        "supplier_id",
        "supplier_name",
        "supplier_city",
        "supplier_country",
        F.round("total_revenue", 2).alias("total_revenue"),
        "total_quantity",
        "orders_count",
        F.round("avg_product_price", 2).alias("avg_product_price"),
        F.round("supplier_country_total_revenue", 2).alias(
            "supplier_country_total_revenue"
        ),
        "supplier_country_orders_count",
        "revenue_rank",
        "is_top_5_supplier",
    )
)

jdbc_write(supplier_sales_mart, "supplier_sales")

quality_rating_desc_window = Window.orderBy(
    F.desc("avg_rating"), F.desc("reviews_count")
)
quality_rating_asc_window = Window.orderBy(F.asc("avg_rating"), F.desc("reviews_count"))
quality_reviews_window = Window.orderBy(
    F.desc("reviews_count"), F.desc("total_quantity")
)

quality_stats = sales_products.groupBy("product_id", "product_name").agg(
    F.avg("product_rating").alias("avg_rating"),
    F.max("product_reviews").alias("reviews_count"),
    F.sum("sale_quantity").alias("total_quantity"),
    F.sum("sale_total_price").alias("total_revenue"),
)

rating_sales_correlation = quality_stats.agg(
    F.corr("avg_rating", "total_quantity").alias("rating_sales_correlation")
)

product_quality_mart = (
    quality_stats.crossJoin(rating_sales_correlation)
    .withColumn("rating_desc_rank", F.row_number().over(quality_rating_desc_window))
    .withColumn("rating_asc_rank", F.row_number().over(quality_rating_asc_window))
    .withColumn("reviews_rank", F.row_number().over(quality_reviews_window))
    .withColumn("has_highest_rating", F.col("rating_desc_rank") == F.lit(1))
    .withColumn("has_lowest_rating", F.col("rating_asc_rank") == F.lit(1))
    .select(
        "product_id",
        "product_name",
        F.round("avg_rating", 2).alias("avg_rating"),
        "reviews_count",
        "total_quantity",
        F.round("total_revenue", 2).alias("total_revenue"),
        F.round("rating_sales_correlation", 4).alias("rating_sales_correlation"),
        "rating_desc_rank",
        "rating_asc_rank",
        "reviews_rank",
        "has_highest_rating",
        "has_lowest_rating",
    )
)

jdbc_write(product_quality_mart, "product_quality")

sales_products.unpersist()
products.unpersist()
sales.unpersist()
spark.stop()
