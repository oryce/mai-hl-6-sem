#!/usr/bin/env python3
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("spark_normalize").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

JDBC_OPTIONS = {
    "url": os.getenv("JDBC_URL"),
    "user": os.getenv("JDBC_USER"),
    "password": os.getenv("JDBC_PASSWORD"),
    "driver": os.getenv("JDBC_DRIVER"),
}


def jdbc_read(table_or_query: str) -> DataFrame:
    return (
        spark.read.format("jdbc")
        .options(**JDBC_OPTIONS)
        .option("dbtable", table_or_query)
        .load()
    )


def jdbc_append(df: DataFrame, table: str) -> None:
    (
        df.write.format("jdbc")
        .options(**JDBC_OPTIONS)
        .option("dbtable", table)
        .mode("append")
        .save()
    )


mock_data = jdbc_read("mock_data").cache()

customers_src = (
    mock_data.select(
        F.col("customer_first_name").alias("first_name"),
        F.col("customer_last_name").alias("last_name"),
        F.col("customer_age").cast("int").alias("age"),
        F.col("customer_email").alias("email"),
        F.col("customer_country").alias("country"),
        F.col("customer_postal_code").alias("postal_code"),
    )
    .filter(F.col("email").isNotNull())
    .dropDuplicates(["email"])
)

jdbc_append(customers_src, "customers")
customers = jdbc_read("customers").select("id", "email")


pet_breeds_src = (
    mock_data.select(F.col("customer_pet_breed").alias("name"))
    .filter(F.col("name").isNotNull())
    .dropDuplicates(["name"])
)
jdbc_append(pet_breeds_src, "pet_breeds")
pet_breeds = jdbc_read("pet_breeds").select("id", "name")

pet_types_src = (
    mock_data.select(F.col("customer_pet_type").alias("name"))
    .filter(F.col("name").isNotNull())
    .dropDuplicates(["name"])
)
jdbc_append(pet_types_src, "pet_types")
pet_types = jdbc_read("pet_types").select("id", "name")

customer_pets_src = (
    mock_data.alias("m")
    .join(customers.alias("c"), F.col("m.customer_email") == F.col("c.email"), "inner")
    .join(
        pet_breeds.alias("b"), F.col("m.customer_pet_breed") == F.col("b.name"), "inner"
    )
    .join(
        pet_types.alias("t"), F.col("m.customer_pet_type") == F.col("t.name"), "inner"
    )
    .select(
        F.col("c.id").alias("customer_id"),
        F.col("m.customer_pet_name").alias("name"),
        F.col("b.id").alias("breed_id"),
        F.col("t.id").alias("type_id"),
    )
    .dropDuplicates(["customer_id", "name", "breed_id", "type_id"])
)

jdbc_append(customer_pets_src, "customer_pets")

sellers_src = (
    mock_data.select(
        F.col("seller_first_name").alias("first_name"),
        F.col("seller_last_name").alias("last_name"),
        F.col("seller_email").alias("email"),
        F.col("seller_country").alias("country"),
        F.col("seller_postal_code").alias("postal_code"),
    )
    .filter(F.col("email").isNotNull())
    .dropDuplicates(["email"])
)

jdbc_append(sellers_src, "sellers")
sellers = jdbc_read("sellers").select("id", "email")


product_brands_src = (
    mock_data.select(F.col("product_brand").alias("name"))
    .filter(F.col("name").isNotNull())
    .dropDuplicates(["name"])
)
jdbc_append(product_brands_src, "product_brands")
product_brands = jdbc_read("product_brands").select("id", "name")

product_categories_src = (
    mock_data.select(F.col("product_category").alias("name"))
    .filter(F.col("name").isNotNull())
    .dropDuplicates(["name"])
)
jdbc_append(product_categories_src, "product_categories")
product_categories = jdbc_read("product_categories").select("id", "name")

product_colors_src = (
    mock_data.select(F.col("product_color").alias("name"))
    .filter(F.col("name").isNotNull())
    .dropDuplicates(["name"])
)
jdbc_append(product_colors_src, "product_colors")
product_colors = jdbc_read("product_colors").select("id", "name")

product_materials_src = (
    mock_data.select(F.col("product_material").alias("name"))
    .filter(F.col("name").isNotNull())
    .dropDuplicates(["name"])
)
jdbc_append(product_materials_src, "product_materials")
product_materials = jdbc_read("product_materials").select("id", "name")

product_pet_categories_src = (
    mock_data.select(F.col("pet_category").alias("name"))
    .filter(F.col("name").isNotNull())
    .dropDuplicates(["name"])
)
jdbc_append(product_pet_categories_src, "product_pet_categories")
product_pet_categories = jdbc_read("product_pet_categories").select("id", "name")

product_sizes_src = (
    mock_data.select(F.col("product_size").alias("name"))
    .filter(F.col("name").isNotNull())
    .dropDuplicates(["name"])
)
jdbc_append(product_sizes_src, "product_sizes")
product_sizes = jdbc_read("product_sizes").select("id", "name")


product_suppliers_src = (
    mock_data.select(
        F.col("supplier_name").alias("name"),
        F.col("supplier_contact").alias("contact"),
        F.col("supplier_email").alias("email"),
        F.col("supplier_phone").alias("phone"),
        F.col("supplier_address").alias("address"),
        F.col("supplier_city").alias("city"),
        F.col("supplier_country").alias("country"),
    )
    .filter(F.col("email").isNotNull())
    .dropDuplicates(["email"])
)

jdbc_append(product_suppliers_src, "product_suppliers")
product_suppliers = jdbc_read("product_suppliers").select("id", "email")

products_src = (
    mock_data.alias("m")
    .join(
        product_suppliers.alias("s"),
        F.col("m.supplier_email") == F.col("s.email"),
        "inner",
    )
    .join(
        product_categories.alias("pc"),
        F.col("m.product_category") == F.col("pc.name"),
        "inner",
    )
    .join(
        product_pet_categories.alias("ppc"),
        F.col("m.pet_category") == F.col("ppc.name"),
        "inner",
    )
    .join(
        product_brands.alias("pb"),
        F.col("m.product_brand") == F.col("pb.name"),
        "inner",
    )
    .join(
        product_colors.alias("pcl"),
        F.col("m.product_color") == F.col("pcl.name"),
        "inner",
    )
    .join(
        product_materials.alias("pm"),
        F.col("m.product_material") == F.col("pm.name"),
        "inner",
    )
    .join(
        product_sizes.alias("psz"),
        F.col("m.product_size") == F.col("psz.name"),
        "inner",
    )
    .select(
        F.col("m.product_name").alias("name"),
        F.col("m.product_description").alias("description"),
        F.col("m.product_price").alias("price"),
        F.col("m.product_quantity").alias("quantity"),
        F.col("s.id").alias("supplier_id"),
        F.col("pc.id").alias("category_id"),
        F.col("ppc.id").alias("pet_category_id"),
        F.col("pb.id").alias("brand_id"),
        F.col("pcl.id").alias("color_id"),
        F.col("pm.id").alias("material_id"),
        F.col("psz.id").alias("size_id"),
        F.col("m.product_weight").alias("weight"),
        F.col("m.product_rating").alias("rating"),
        F.col("m.product_reviews").alias("reviews"),
        F.to_date(F.col("m.product_release_date")).alias("release_date"),
        F.to_date(F.col("m.product_expiry_date")).alias("expiry_date"),
    )
    .dropDuplicates(["name", "description", "release_date", "expiry_date"])
)

jdbc_append(products_src, "products")

products = jdbc_read("products").select(
    "id", "name", "description", "release_date", "expiry_date"
)

stores_src = (
    mock_data.select(
        F.col("store_name").alias("name"),
        F.col("store_location").alias("location"),
        F.col("store_city").alias("city"),
        F.col("store_state").alias("state"),
        F.col("store_country").alias("country"),
        F.col("store_email").alias("email"),
        F.col("store_phone").alias("phone"),
    )
    .filter(F.col("email").isNotNull())
    .dropDuplicates(["email"])
)

jdbc_append(stores_src, "stores")
stores = jdbc_read("stores").select("id", "email")

sales_src = (
    mock_data.alias("m")
    .join(customers.alias("c"), F.col("m.customer_email") == F.col("c.email"), "inner")
    .join(sellers.alias("s"), F.col("m.seller_email") == F.col("s.email"), "inner")
    .join(
        products.alias("p"),
        (
            (F.col("m.product_name") == F.col("p.name"))
            & (F.col("m.product_description") == F.col("p.description"))
            & (F.to_date(F.col("m.product_release_date")) == F.col("p.release_date"))
            & (F.to_date(F.col("m.product_expiry_date")) == F.col("p.expiry_date"))
        ),
        "inner",
    )
    .join(stores.alias("st"), F.col("m.store_email") == F.col("st.email"), "inner")
    .select(
        F.col("c.id").alias("customer_id"),
        F.to_date(F.col("m.sale_date")).alias("date"),
        F.col("s.id").alias("seller_id"),
        F.col("st.id").alias("store_id"),
        F.col("p.id").alias("product_id"),
        F.col("m.sale_quantity").alias("quantity"),
        F.col("m.sale_total_price").alias("total_price"),
    )
    .dropDuplicates(
        [
            "customer_id",
            "date",
            "seller_id",
            "store_id",
            "product_id",
            "quantity",
            "total_price",
        ]
    )
)

jdbc_append(sales_src, "sales")

mock_data.unpersist()
spark.stop()
