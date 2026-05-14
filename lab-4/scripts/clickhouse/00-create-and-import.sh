#!/bin/sh

set -eu

clickhouse() {
  clickhouse-client \
    --user "$CLICKHOUSE_USER" \
    --password "$CLICKHOUSE_PASSWORD" \
    "${@}"
}

clickhouse --multiquery <<-EOSQL
CREATE DATABASE IF NOT EXISTS ${CLICKHOUSE_DB};
USE ${CLICKHOUSE_DB};

CREATE TABLE IF NOT EXISTS mock_data(
  id                    Int32,

  customer_first_name   String,
  customer_last_name    String,
  customer_age          Int16,
  customer_email        String,
  customer_country      String,
  customer_postal_code  String,
  customer_pet_type     String,
  customer_pet_name     String,
  customer_pet_breed    String,

  seller_first_name     String,
  seller_last_name      String,
  seller_email          String,
  seller_country        String,
  seller_postal_code    String,

  product_name          String,
  product_category      String,
  product_price         Decimal(10, 2),
  product_quantity      Int32,
  product_weight        Decimal(6, 2),
  product_color         String,
  product_size          String,
  product_brand         String,
  product_material      String,
  product_description   String,
  product_rating        Decimal(2, 1),
  product_reviews       Int32,
  product_release_date  String,
  product_expiry_date   String,

  sale_date             String,
  sale_customer_id      Int32,
  sale_seller_id        Int32,
  sale_product_id       Int32,
  sale_quantity         Int32,
  sale_total_price      Decimal(10, 2),

  store_name            String,
  store_location        String,
  store_city            String,
  store_state           String,
  store_country         String,
  store_phone           String,
  store_email           String,

  pet_category          String,

  supplier_name         String,
  supplier_contact      String,
  supplier_email        String,
  supplier_phone        String,
  supplier_address      String,
  supplier_city         String,
  supplier_country      String
)
ENGINE = MergeTree
ORDER BY id;
EOSQL

for file in \
  "/source/MOCK_DATA.csv" \
  "/source/MOCK_DATA (1).csv" \
  "/source/MOCK_DATA (2).csv" \
  "/source/MOCK_DATA (3).csv" \
  "/source/MOCK_DATA (4).csv"
do
  clickhouse \
    --database "$CLICKHOUSE_DB" \
    --query "INSERT INTO mock_data FORMAT CSVWithNames" < "$file"
done
