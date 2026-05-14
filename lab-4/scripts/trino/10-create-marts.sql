DROP TABLE IF EXISTS clickhouse.spark.product_quality;
DROP TABLE IF EXISTS clickhouse.spark.supplier_sales;
DROP TABLE IF EXISTS clickhouse.spark.store_sales;
DROP TABLE IF EXISTS clickhouse.spark.time_sales;
DROP TABLE IF EXISTS clickhouse.spark.customer_sales;
DROP TABLE IF EXISTS clickhouse.spark.product_sales;

CREATE TABLE clickhouse.spark.product_sales(
  product_id              INTEGER,
  product_name            VARCHAR,
  category_id             INTEGER,
  category_name           VARCHAR,
  total_revenue           DOUBLE,
  total_quantity          BIGINT,
  sales_count             BIGINT,
  category_total_revenue  DOUBLE,
  avg_rating              DOUBLE,
  reviews_count           BIGINT,
  sales_rank              INTEGER,
  is_top_10_by_quantity   BOOLEAN
)
WITH (
  engine = 'MergeTree'
);

INSERT INTO clickhouse.spark.product_sales
WITH sales_products AS (
  SELECT
    s.id AS sale_id,
    s.product_id,
    p.name AS product_name,
    p.category_id,
    c.name AS category_name,
    CAST(s.total_price AS DOUBLE) AS sale_total_price,
    CAST(s.quantity AS BIGINT) AS sale_quantity,
    CAST(p.rating AS DOUBLE) AS product_rating,
    CAST(p.reviews AS BIGINT) AS product_reviews
  FROM clickhouse.spark.sales s
  JOIN clickhouse.spark.products p ON s.product_id = p.id
  JOIN clickhouse.spark.product_categories c ON p.category_id = c.id
),
product_agg AS (
  SELECT
    product_id,
    product_name,
    category_id,
    category_name,
    sum(sale_total_price) AS total_revenue,
    sum(sale_quantity) AS total_quantity,
    count(sale_id) AS sales_count,
    avg(product_rating) AS avg_rating,
    max(product_reviews) AS reviews_count
  FROM sales_products
  GROUP BY product_id, product_name, category_id, category_name
),
ranked AS (
  SELECT
    *,
    sum(total_revenue) OVER (PARTITION BY category_id) AS category_total_revenue,
    row_number() OVER (ORDER BY total_quantity DESC, total_revenue DESC) AS sales_rank
  FROM product_agg
)
SELECT
  product_id,
  product_name,
  category_id,
  category_name,
  round(total_revenue, 2) AS total_revenue,
  total_quantity,
  sales_count,
  round(category_total_revenue, 2) AS category_total_revenue,
  round(avg_rating, 2) AS avg_rating,
  reviews_count,
  CAST(sales_rank AS INTEGER) AS sales_rank,
  sales_rank <= 10 AS is_top_10_by_quantity
FROM ranked;

CREATE TABLE clickhouse.spark.customer_sales(
  customer_id              INTEGER,
  customer_first_name      VARCHAR,
  customer_last_name       VARCHAR,
  customer_email           VARCHAR,
  customer_country         VARCHAR,
  total_purchase_amount    DOUBLE,
  orders_count             BIGINT,
  avg_check                DOUBLE,
  country_customers_count  BIGINT,
  purchase_rank            INTEGER,
  is_top_10_customer       BOOLEAN
)
WITH (
  engine = 'MergeTree'
);

INSERT INTO clickhouse.spark.customer_sales
WITH customer_base AS (
  SELECT
    s.id AS sale_id,
    s.customer_id,
    c.first_name AS customer_first_name,
    c.last_name AS customer_last_name,
    c.email AS customer_email,
    c.country AS customer_country,
    CAST(s.total_price AS DOUBLE) AS sale_total_price
  FROM clickhouse.spark.sales s
  JOIN clickhouse.spark.customers c ON s.customer_id = c.id
),
customer_countries AS (
  SELECT
    country AS customer_country,
    count(DISTINCT id) AS country_customers_count
  FROM clickhouse.spark.customers
  GROUP BY country
),
customer_agg AS (
  SELECT
    customer_id,
    customer_first_name,
    customer_last_name,
    customer_email,
    customer_country,
    sum(sale_total_price) AS total_purchase_amount,
    count(sale_id) AS orders_count,
    avg(sale_total_price) AS avg_check
  FROM customer_base
  GROUP BY
    customer_id,
    customer_first_name,
    customer_last_name,
    customer_email,
    customer_country
),
ranked AS (
  SELECT
    ca.*,
    cc.country_customers_count,
    row_number() OVER (ORDER BY total_purchase_amount DESC) AS purchase_rank
  FROM customer_agg ca
  LEFT JOIN customer_countries cc ON ca.customer_country = cc.customer_country
)
SELECT
  customer_id,
  customer_first_name,
  customer_last_name,
  customer_email,
  customer_country,
  round(total_purchase_amount, 2) AS total_purchase_amount,
  orders_count,
  round(avg_check, 2) AS avg_check,
  country_customers_count,
  CAST(purchase_rank AS INTEGER) AS purchase_rank,
  purchase_rank <= 10 AS is_top_10_customer
FROM ranked;

CREATE TABLE clickhouse.spark.time_sales(
  period_start            DATE,
  sale_year               INTEGER,
  sale_month              INTEGER,
  monthly_revenue         DOUBLE,
  monthly_quantity        BIGINT,
  monthly_orders          BIGINT,
  monthly_avg_order       DOUBLE,
  yearly_revenue          DOUBLE,
  yearly_orders           BIGINT,
  previous_month_revenue  DOUBLE,
  monthly_revenue_delta   DOUBLE
)
WITH (
  engine = 'MergeTree'
);

INSERT INTO clickhouse.spark.time_sales
WITH monthly_sales AS (
  SELECT
    CAST(date_trunc('month', date) AS DATE) AS period_start,
    year(date) AS sale_year,
    month(date) AS sale_month,
    sum(CAST(total_price AS DOUBLE)) AS monthly_revenue,
    sum(CAST(quantity AS BIGINT)) AS monthly_quantity,
    count(id) AS monthly_orders,
    avg(CAST(total_price AS DOUBLE)) AS monthly_avg_order
  FROM clickhouse.spark.sales
  GROUP BY
    CAST(date_trunc('month', date) AS DATE),
    year(date),
    month(date)
),
yearly_sales AS (
  SELECT
    sale_year,
    sum(monthly_revenue) AS yearly_revenue,
    sum(monthly_orders) AS yearly_orders
  FROM monthly_sales
  GROUP BY sale_year
),
with_previous AS (
  SELECT
    ms.*,
    ys.yearly_revenue,
    ys.yearly_orders,
    lag(ms.monthly_revenue) OVER (ORDER BY ms.period_start) AS previous_month_revenue
  FROM monthly_sales ms
  JOIN yearly_sales ys ON ms.sale_year = ys.sale_year
)
SELECT
  period_start,
  sale_year,
  sale_month,
  round(monthly_revenue, 2) AS monthly_revenue,
  monthly_quantity,
  monthly_orders,
  round(monthly_avg_order, 2) AS monthly_avg_order,
  round(yearly_revenue, 2) AS yearly_revenue,
  yearly_orders,
  round(previous_month_revenue, 2) AS previous_month_revenue,
  round(monthly_revenue - previous_month_revenue, 2) AS monthly_revenue_delta
FROM with_previous;

CREATE TABLE clickhouse.spark.store_sales(
  store_id                    INTEGER,
  store_name                  VARCHAR,
  store_city                  VARCHAR,
  store_country               VARCHAR,
  total_revenue               DOUBLE,
  total_quantity              BIGINT,
  orders_count                BIGINT,
  avg_check                   DOUBLE,
  city_country_total_revenue  DOUBLE,
  city_country_orders_count   BIGINT,
  revenue_rank                INTEGER,
  is_top_5_store              BOOLEAN
)
WITH (
  engine = 'MergeTree'
);

INSERT INTO clickhouse.spark.store_sales
WITH store_base AS (
  SELECT
    s.id AS sale_id,
    s.store_id,
    st.name AS store_name,
    st.city AS store_city,
    st.country AS store_country,
    CAST(s.total_price AS DOUBLE) AS sale_total_price,
    CAST(s.quantity AS BIGINT) AS sale_quantity
  FROM clickhouse.spark.sales s
  JOIN clickhouse.spark.stores st ON s.store_id = st.id
),
store_agg AS (
  SELECT
    store_id,
    store_name,
    store_city,
    store_country,
    sum(sale_total_price) AS total_revenue,
    sum(sale_quantity) AS total_quantity,
    count(sale_id) AS orders_count,
    avg(sale_total_price) AS avg_check
  FROM store_base
  GROUP BY store_id, store_name, store_city, store_country
),
city_country_agg AS (
  SELECT
    store_city,
    store_country,
    sum(sale_total_price) AS city_country_total_revenue,
    count(sale_id) AS city_country_orders_count
  FROM store_base
  GROUP BY store_city, store_country
),
ranked AS (
  SELECT
    sa.*,
    cca.city_country_total_revenue,
    cca.city_country_orders_count,
    row_number() OVER (ORDER BY sa.total_revenue DESC) AS revenue_rank
  FROM store_agg sa
  LEFT JOIN city_country_agg cca
    ON sa.store_city = cca.store_city
    AND sa.store_country = cca.store_country
)
SELECT
  store_id,
  store_name,
  store_city,
  store_country,
  round(total_revenue, 2) AS total_revenue,
  total_quantity,
  orders_count,
  round(avg_check, 2) AS avg_check,
  round(city_country_total_revenue, 2) AS city_country_total_revenue,
  city_country_orders_count,
  CAST(revenue_rank AS INTEGER) AS revenue_rank,
  revenue_rank <= 5 AS is_top_5_store
FROM ranked;

CREATE TABLE clickhouse.spark.supplier_sales(
  supplier_id                     INTEGER,
  supplier_name                   VARCHAR,
  supplier_city                   VARCHAR,
  supplier_country                VARCHAR,
  total_revenue                   DOUBLE,
  total_quantity                  BIGINT,
  orders_count                    BIGINT,
  avg_product_price               DOUBLE,
  supplier_country_total_revenue  DOUBLE,
  supplier_country_orders_count   BIGINT,
  revenue_rank                    INTEGER,
  is_top_5_supplier               BOOLEAN
)
WITH (
  engine = 'MergeTree'
);

INSERT INTO clickhouse.spark.supplier_sales
WITH sales_products AS (
  SELECT
    s.id AS sale_id,
    p.supplier_id,
    CAST(s.total_price AS DOUBLE) AS sale_total_price,
    CAST(s.quantity AS BIGINT) AS sale_quantity
  FROM clickhouse.spark.sales s
  JOIN clickhouse.spark.products p ON s.product_id = p.id
),
supplier_base AS (
  SELECT
    sp.sale_id,
    sp.supplier_id,
    ps.name AS supplier_name,
    ps.city AS supplier_city,
    ps.country AS supplier_country,
    sp.sale_total_price,
    sp.sale_quantity
  FROM sales_products sp
  JOIN clickhouse.spark.product_suppliers ps ON sp.supplier_id = ps.id
),
supplier_product_prices AS (
  SELECT
    supplier_id,
    avg(CAST(price AS DOUBLE)) AS avg_product_price
  FROM clickhouse.spark.products
  GROUP BY supplier_id
),
supplier_agg AS (
  SELECT
    supplier_id,
    supplier_name,
    supplier_city,
    supplier_country,
    sum(sale_total_price) AS total_revenue,
    sum(sale_quantity) AS total_quantity,
    count(sale_id) AS orders_count
  FROM supplier_base
  GROUP BY supplier_id, supplier_name, supplier_city, supplier_country
),
country_agg AS (
  SELECT
    supplier_country,
    sum(sale_total_price) AS supplier_country_total_revenue,
    count(sale_id) AS supplier_country_orders_count
  FROM supplier_base
  GROUP BY supplier_country
),
ranked AS (
  SELECT
    sa.*,
    spp.avg_product_price,
    ca.supplier_country_total_revenue,
    ca.supplier_country_orders_count,
    row_number() OVER (ORDER BY sa.total_revenue DESC) AS revenue_rank
  FROM supplier_agg sa
  LEFT JOIN supplier_product_prices spp ON sa.supplier_id = spp.supplier_id
  LEFT JOIN country_agg ca ON sa.supplier_country = ca.supplier_country
)
SELECT
  supplier_id,
  supplier_name,
  supplier_city,
  supplier_country,
  round(total_revenue, 2) AS total_revenue,
  total_quantity,
  orders_count,
  round(avg_product_price, 2) AS avg_product_price,
  round(supplier_country_total_revenue, 2) AS supplier_country_total_revenue,
  supplier_country_orders_count,
  CAST(revenue_rank AS INTEGER) AS revenue_rank,
  revenue_rank <= 5 AS is_top_5_supplier
FROM ranked;

CREATE TABLE clickhouse.spark.product_quality(
  product_id                INTEGER,
  product_name              VARCHAR,
  avg_rating                DOUBLE,
  reviews_count             BIGINT,
  total_quantity            BIGINT,
  total_revenue             DOUBLE,
  rating_sales_correlation  DOUBLE,
  rating_desc_rank          INTEGER,
  rating_asc_rank           INTEGER,
  reviews_rank              INTEGER,
  has_highest_rating        BOOLEAN,
  has_lowest_rating         BOOLEAN
)
WITH (
  engine = 'MergeTree'
);

INSERT INTO clickhouse.spark.product_quality
WITH sales_products AS (
  SELECT
    s.product_id,
    p.name AS product_name,
    CAST(p.rating AS DOUBLE) AS product_rating,
    CAST(p.reviews AS BIGINT) AS product_reviews,
    CAST(s.quantity AS BIGINT) AS sale_quantity,
    CAST(s.total_price AS DOUBLE) AS sale_total_price
  FROM clickhouse.spark.sales s
  JOIN clickhouse.spark.products p ON s.product_id = p.id
),
quality_stats AS (
  SELECT
    product_id,
    product_name,
    avg(product_rating) AS avg_rating,
    max(product_reviews) AS reviews_count,
    sum(sale_quantity) AS total_quantity,
    sum(sale_total_price) AS total_revenue
  FROM sales_products
  GROUP BY product_id, product_name
),
rating_sales_correlation AS (
  SELECT corr(avg_rating, CAST(total_quantity AS DOUBLE)) AS rating_sales_correlation
  FROM quality_stats
),
ranked AS (
  SELECT
    qs.*,
    rsc.rating_sales_correlation,
    row_number() OVER (ORDER BY avg_rating DESC, reviews_count DESC) AS rating_desc_rank,
    row_number() OVER (ORDER BY avg_rating ASC, reviews_count DESC) AS rating_asc_rank,
    row_number() OVER (ORDER BY reviews_count DESC, total_quantity DESC) AS reviews_rank
  FROM quality_stats qs
  CROSS JOIN rating_sales_correlation rsc
)
SELECT
  product_id,
  product_name,
  round(avg_rating, 2) AS avg_rating,
  reviews_count,
  total_quantity,
  round(total_revenue, 2) AS total_revenue,
  round(rating_sales_correlation, 4) AS rating_sales_correlation,
  CAST(rating_desc_rank AS INTEGER) AS rating_desc_rank,
  CAST(rating_asc_rank AS INTEGER) AS rating_asc_rank,
  CAST(reviews_rank AS INTEGER) AS reviews_rank,
  rating_desc_rank = 1 AS has_highest_rating,
  rating_asc_rank = 1 AS has_lowest_rating
FROM ranked;
