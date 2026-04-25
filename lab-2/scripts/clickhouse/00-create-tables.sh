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

CREATE TABLE IF NOT EXISTS product_sales(
  product_id              Int32,
  product_name            String,
  category_id             Int32,
  category_name           String,
  total_revenue           Float64,
  total_quantity          Int64,
  sales_count             Int64,
  category_total_revenue  Float64,
  avg_rating              Float64,
  reviews_count           Int64,
  sales_rank              Int32,
  is_top_10_by_quantity   Boolean
)
ENGINE = MergeTree
ORDER BY (sales_rank, product_id);

CREATE TABLE IF NOT EXISTS customer_sales(
  customer_id             Int32,
  customer_first_name     String,
  customer_last_name      String,
  customer_email          String,
  customer_country        String,
  total_purchase_amount   Float64,
  orders_count            Int64,
  avg_check               Float64,
  country_customers_count Int64,
  purchase_rank           Int32,
  is_top_10_customer      Boolean
)
ENGINE = MergeTree
ORDER BY (purchase_rank, customer_id);

CREATE TABLE IF NOT EXISTS time_sales(
  period_start            Date,
  sale_year               Int32,
  sale_month              Int32,
  monthly_revenue         Float64,
  monthly_quantity        Int64,
  monthly_orders          Int64,
  monthly_avg_order       Float64,
  yearly_revenue          Float64,
  yearly_orders           Int64,
  previous_month_revenue  Nullable(Float64),
  monthly_revenue_delta   Nullable(Float64)
)
ENGINE = MergeTree
ORDER BY (period_start, sale_year, sale_month);

CREATE TABLE IF NOT EXISTS store_sales(
  store_id                    Int32,
  store_name                  String,
  store_city                  String,
  store_country               String,
  total_revenue               Float64,
  total_quantity              Int64,
  orders_count                Int64,
  avg_check                   Float64,
  city_country_total_revenue  Float64,
  city_country_orders_count   Int64,
  revenue_rank                Int32,
  is_top_5_store              Boolean
)
ENGINE = MergeTree
ORDER BY (revenue_rank, store_id);

CREATE TABLE IF NOT EXISTS supplier_sales(
  supplier_id                     Int32,
  supplier_name                   String,
  supplier_city                   String,
  supplier_country                String,
  total_revenue                   Float64,
  total_quantity                  Int64,
  orders_count                    Int64,
  avg_product_price               Float64,
  supplier_country_total_revenue  Float64,
  supplier_country_orders_count   Int64,
  revenue_rank                    Int32,
  is_top_5_supplier               Boolean
)
ENGINE = MergeTree
ORDER BY (revenue_rank, supplier_id);

CREATE TABLE IF NOT EXISTS product_quality(
  product_id                Int32,
  product_name              String,
  avg_rating                Float64,
  reviews_count             Int64,
  total_quantity            Int64,
  total_revenue             Float64,
  rating_sales_correlation  Nullable(Float64),
  rating_desc_rank          Int32,
  rating_asc_rank           Int32,
  reviews_rank              Int32,
  has_highest_rating        Boolean,
  has_lowest_rating         Boolean
)
ENGINE = MergeTree
ORDER BY (reviews_rank, product_id);
EOSQL
