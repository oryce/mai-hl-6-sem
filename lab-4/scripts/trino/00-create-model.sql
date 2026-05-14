DROP TABLE IF EXISTS clickhouse.spark.sales;
DROP TABLE IF EXISTS clickhouse.spark.stores;
DROP TABLE IF EXISTS clickhouse.spark.products;
DROP TABLE IF EXISTS clickhouse.spark.product_suppliers;
DROP TABLE IF EXISTS clickhouse.spark.product_sizes;
DROP TABLE IF EXISTS clickhouse.spark.product_pet_categories;
DROP TABLE IF EXISTS clickhouse.spark.product_materials;
DROP TABLE IF EXISTS clickhouse.spark.product_colors;
DROP TABLE IF EXISTS clickhouse.spark.product_categories;
DROP TABLE IF EXISTS clickhouse.spark.product_brands;
DROP TABLE IF EXISTS clickhouse.spark.sellers;
DROP TABLE IF EXISTS clickhouse.spark.customer_pets;
DROP TABLE IF EXISTS clickhouse.spark.pet_types;
DROP TABLE IF EXISTS clickhouse.spark.pet_breeds;
DROP TABLE IF EXISTS clickhouse.spark.customers;
DROP TABLE IF EXISTS clickhouse.spark.stg_mock_data;

CREATE TABLE clickhouse.spark.stg_mock_data(
  source_system          VARCHAR,
  source_id              INTEGER,

  customer_first_name    VARCHAR,
  customer_last_name     VARCHAR,
  customer_age           SMALLINT,
  customer_email         VARCHAR,
  customer_country       VARCHAR,
  customer_postal_code   VARCHAR,
  customer_pet_type      VARCHAR,
  customer_pet_name      VARCHAR,
  customer_pet_breed     VARCHAR,

  seller_first_name      VARCHAR,
  seller_last_name       VARCHAR,
  seller_email           VARCHAR,
  seller_country         VARCHAR,
  seller_postal_code     VARCHAR,

  product_name           VARCHAR,
  product_category       VARCHAR,
  product_price          DECIMAL(10, 2),
  product_quantity       INTEGER,
  product_weight         DECIMAL(6, 2),
  product_color          VARCHAR,
  product_size           VARCHAR,
  product_brand          VARCHAR,
  product_material       VARCHAR,
  product_description    VARCHAR,
  product_rating         DECIMAL(2, 1),
  product_reviews        INTEGER,
  product_release_date   DATE,
  product_expiry_date    DATE,

  sale_date              DATE,
  sale_customer_id       INTEGER,
  sale_seller_id         INTEGER,
  sale_product_id        INTEGER,
  sale_quantity          INTEGER,
  sale_total_price       DECIMAL(10, 2),

  store_name             VARCHAR,
  store_location         VARCHAR,
  store_city             VARCHAR,
  store_state            VARCHAR,
  store_country          VARCHAR,
  store_phone            VARCHAR,
  store_email            VARCHAR,

  pet_category           VARCHAR,

  supplier_name          VARCHAR,
  supplier_contact       VARCHAR,
  supplier_email         VARCHAR,
  supplier_phone         VARCHAR,
  supplier_address       VARCHAR,
  supplier_city          VARCHAR,
  supplier_country       VARCHAR
)
WITH (
  engine = 'MergeTree'
);

INSERT INTO clickhouse.spark.stg_mock_data
WITH raw AS (
  SELECT 'clickhouse' AS source_system, *
  FROM clickhouse.spark.mock_data
  UNION ALL
  SELECT 'postgresql' AS source_system, *
  FROM postgresql.public.mock_data
)
SELECT
  source_system,
  CAST(id AS INTEGER) AS source_id,

  NULLIF(TRIM(customer_first_name), '') AS customer_first_name,
  NULLIF(TRIM(customer_last_name), '') AS customer_last_name,
  CAST(customer_age AS SMALLINT) AS customer_age,
  NULLIF(TRIM(customer_email), '') AS customer_email,
  NULLIF(TRIM(customer_country), '') AS customer_country,
  NULLIF(TRIM(customer_postal_code), '') AS customer_postal_code,
  NULLIF(TRIM(customer_pet_type), '') AS customer_pet_type,
  NULLIF(TRIM(customer_pet_name), '') AS customer_pet_name,
  NULLIF(TRIM(customer_pet_breed), '') AS customer_pet_breed,

  NULLIF(TRIM(seller_first_name), '') AS seller_first_name,
  NULLIF(TRIM(seller_last_name), '') AS seller_last_name,
  NULLIF(TRIM(seller_email), '') AS seller_email,
  NULLIF(TRIM(seller_country), '') AS seller_country,
  NULLIF(TRIM(seller_postal_code), '') AS seller_postal_code,

  NULLIF(TRIM(product_name), '') AS product_name,
  NULLIF(TRIM(product_category), '') AS product_category,
  CAST(product_price AS DECIMAL(10, 2)) AS product_price,
  CAST(product_quantity AS INTEGER) AS product_quantity,
  CAST(product_weight AS DECIMAL(6, 2)) AS product_weight,
  NULLIF(TRIM(product_color), '') AS product_color,
  NULLIF(TRIM(product_size), '') AS product_size,
  NULLIF(TRIM(product_brand), '') AS product_brand,
  NULLIF(TRIM(product_material), '') AS product_material,
  NULLIF(TRIM(product_description), '') AS product_description,
  CAST(product_rating AS DECIMAL(2, 1)) AS product_rating,
  CAST(product_reviews AS INTEGER) AS product_reviews,
  TRY(CAST(date_parse(product_release_date, '%c/%e/%Y') AS DATE)) AS product_release_date,
  TRY(CAST(date_parse(product_expiry_date, '%c/%e/%Y') AS DATE)) AS product_expiry_date,

  TRY(CAST(date_parse(sale_date, '%c/%e/%Y') AS DATE)) AS sale_date,
  CAST(sale_customer_id AS INTEGER) AS sale_customer_id,
  CAST(sale_seller_id AS INTEGER) AS sale_seller_id,
  CAST(sale_product_id AS INTEGER) AS sale_product_id,
  CAST(sale_quantity AS INTEGER) AS sale_quantity,
  CAST(sale_total_price AS DECIMAL(10, 2)) AS sale_total_price,

  NULLIF(TRIM(store_name), '') AS store_name,
  NULLIF(TRIM(store_location), '') AS store_location,
  NULLIF(TRIM(store_city), '') AS store_city,
  NULLIF(TRIM(store_state), '') AS store_state,
  NULLIF(TRIM(store_country), '') AS store_country,
  NULLIF(TRIM(store_phone), '') AS store_phone,
  NULLIF(TRIM(store_email), '') AS store_email,

  NULLIF(TRIM(pet_category), '') AS pet_category,

  NULLIF(TRIM(supplier_name), '') AS supplier_name,
  NULLIF(TRIM(supplier_contact), '') AS supplier_contact,
  NULLIF(TRIM(supplier_email), '') AS supplier_email,
  NULLIF(TRIM(supplier_phone), '') AS supplier_phone,
  NULLIF(TRIM(supplier_address), '') AS supplier_address,
  NULLIF(TRIM(supplier_city), '') AS supplier_city,
  NULLIF(TRIM(supplier_country), '') AS supplier_country
FROM raw;

CREATE TABLE clickhouse.spark.customers(
  id           INTEGER,
  first_name   VARCHAR,
  last_name    VARCHAR,
  age          SMALLINT,
  email        VARCHAR,
  country      VARCHAR,
  postal_code  VARCHAR
)
WITH (
  engine = 'MergeTree'
);

INSERT INTO clickhouse.spark.customers
SELECT
  CAST(row_number() OVER (ORDER BY customer_email) AS INTEGER) AS id,
  arbitrary(customer_first_name) AS first_name,
  arbitrary(customer_last_name) AS last_name,
  arbitrary(customer_age) AS age,
  customer_email AS email,
  arbitrary(customer_country) AS country,
  arbitrary(customer_postal_code) AS postal_code
FROM clickhouse.spark.stg_mock_data
WHERE customer_email IS NOT NULL
GROUP BY customer_email;

CREATE TABLE clickhouse.spark.pet_breeds(
  id    INTEGER,
  name  VARCHAR
)
WITH (
  engine = 'MergeTree'
);

INSERT INTO clickhouse.spark.pet_breeds
SELECT
  CAST(row_number() OVER (ORDER BY customer_pet_breed) AS INTEGER) AS id,
  customer_pet_breed AS name
FROM clickhouse.spark.stg_mock_data
WHERE customer_pet_breed IS NOT NULL
GROUP BY customer_pet_breed;

CREATE TABLE clickhouse.spark.pet_types(
  id    INTEGER,
  name  VARCHAR
)
WITH (
  engine = 'MergeTree'
);

INSERT INTO clickhouse.spark.pet_types
SELECT
  CAST(row_number() OVER (ORDER BY customer_pet_type) AS INTEGER) AS id,
  customer_pet_type AS name
FROM clickhouse.spark.stg_mock_data
WHERE customer_pet_type IS NOT NULL
GROUP BY customer_pet_type;

CREATE TABLE clickhouse.spark.customer_pets(
  id           INTEGER,
  customer_id  INTEGER,
  name         VARCHAR,
  breed_id     INTEGER,
  type_id      INTEGER
)
WITH (
  engine = 'MergeTree'
);

INSERT INTO clickhouse.spark.customer_pets
WITH pet_rows AS (
  SELECT DISTINCT
    c.id AS customer_id,
    m.customer_pet_name AS name,
    b.id AS breed_id,
    t.id AS type_id
  FROM clickhouse.spark.stg_mock_data m
  JOIN clickhouse.spark.customers c ON m.customer_email = c.email
  JOIN clickhouse.spark.pet_breeds b ON m.customer_pet_breed = b.name
  JOIN clickhouse.spark.pet_types t ON m.customer_pet_type = t.name
  WHERE m.customer_pet_name IS NOT NULL
)
SELECT
  CAST(row_number() OVER (ORDER BY customer_id, name, breed_id, type_id) AS INTEGER) AS id,
  customer_id,
  name,
  breed_id,
  type_id
FROM pet_rows;

CREATE TABLE clickhouse.spark.sellers(
  id           INTEGER,
  first_name   VARCHAR,
  last_name    VARCHAR,
  email        VARCHAR,
  country      VARCHAR,
  postal_code  VARCHAR
)
WITH (
  engine = 'MergeTree'
);

INSERT INTO clickhouse.spark.sellers
SELECT
  CAST(row_number() OVER (ORDER BY seller_email) AS INTEGER) AS id,
  arbitrary(seller_first_name) AS first_name,
  arbitrary(seller_last_name) AS last_name,
  seller_email AS email,
  arbitrary(seller_country) AS country,
  arbitrary(seller_postal_code) AS postal_code
FROM clickhouse.spark.stg_mock_data
WHERE seller_email IS NOT NULL
GROUP BY seller_email;

CREATE TABLE clickhouse.spark.product_brands(
  id    INTEGER,
  name  VARCHAR
)
WITH (
  engine = 'MergeTree'
);

INSERT INTO clickhouse.spark.product_brands
SELECT
  CAST(row_number() OVER (ORDER BY product_brand) AS INTEGER) AS id,
  product_brand AS name
FROM clickhouse.spark.stg_mock_data
WHERE product_brand IS NOT NULL
GROUP BY product_brand;

CREATE TABLE clickhouse.spark.product_categories(
  id    INTEGER,
  name  VARCHAR
)
WITH (
  engine = 'MergeTree'
);

INSERT INTO clickhouse.spark.product_categories
SELECT
  CAST(row_number() OVER (ORDER BY product_category) AS INTEGER) AS id,
  product_category AS name
FROM clickhouse.spark.stg_mock_data
WHERE product_category IS NOT NULL
GROUP BY product_category;

CREATE TABLE clickhouse.spark.product_colors(
  id    INTEGER,
  name  VARCHAR
)
WITH (
  engine = 'MergeTree'
);

INSERT INTO clickhouse.spark.product_colors
SELECT
  CAST(row_number() OVER (ORDER BY product_color) AS INTEGER) AS id,
  product_color AS name
FROM clickhouse.spark.stg_mock_data
WHERE product_color IS NOT NULL
GROUP BY product_color;

CREATE TABLE clickhouse.spark.product_materials(
  id    INTEGER,
  name  VARCHAR
)
WITH (
  engine = 'MergeTree'
);

INSERT INTO clickhouse.spark.product_materials
SELECT
  CAST(row_number() OVER (ORDER BY product_material) AS INTEGER) AS id,
  product_material AS name
FROM clickhouse.spark.stg_mock_data
WHERE product_material IS NOT NULL
GROUP BY product_material;

CREATE TABLE clickhouse.spark.product_pet_categories(
  id    INTEGER,
  name  VARCHAR
)
WITH (
  engine = 'MergeTree'
);

INSERT INTO clickhouse.spark.product_pet_categories
SELECT
  CAST(row_number() OVER (ORDER BY pet_category) AS INTEGER) AS id,
  pet_category AS name
FROM clickhouse.spark.stg_mock_data
WHERE pet_category IS NOT NULL
GROUP BY pet_category;

CREATE TABLE clickhouse.spark.product_sizes(
  id    INTEGER,
  name  VARCHAR
)
WITH (
  engine = 'MergeTree'
);

INSERT INTO clickhouse.spark.product_sizes
SELECT
  CAST(row_number() OVER (ORDER BY product_size) AS INTEGER) AS id,
  product_size AS name
FROM clickhouse.spark.stg_mock_data
WHERE product_size IS NOT NULL
GROUP BY product_size;

CREATE TABLE clickhouse.spark.product_suppliers(
  id       INTEGER,
  name     VARCHAR,
  contact  VARCHAR,
  email    VARCHAR,
  phone    VARCHAR,
  address  VARCHAR,
  city     VARCHAR,
  country  VARCHAR
)
WITH (
  engine = 'MergeTree'
);

INSERT INTO clickhouse.spark.product_suppliers
SELECT
  CAST(row_number() OVER (ORDER BY supplier_email) AS INTEGER) AS id,
  arbitrary(supplier_name) AS name,
  arbitrary(supplier_contact) AS contact,
  supplier_email AS email,
  arbitrary(supplier_phone) AS phone,
  arbitrary(supplier_address) AS address,
  arbitrary(supplier_city) AS city,
  arbitrary(supplier_country) AS country
FROM clickhouse.spark.stg_mock_data
WHERE supplier_email IS NOT NULL
GROUP BY supplier_email;

CREATE TABLE clickhouse.spark.products(
  id                INTEGER,
  name              VARCHAR,
  description       VARCHAR,
  price             DECIMAL(10, 2),
  quantity          INTEGER,
  supplier_id       INTEGER,
  category_id       INTEGER,
  pet_category_id   INTEGER,
  brand_id          INTEGER,
  color_id          INTEGER,
  material_id       INTEGER,
  size_id           INTEGER,
  weight            DECIMAL(6, 2),
  rating            DECIMAL(2, 1),
  reviews           INTEGER,
  release_date      DATE,
  expiry_date       DATE
)
WITH (
  engine = 'MergeTree'
);

INSERT INTO clickhouse.spark.products
WITH product_rows AS (
  SELECT
    m.product_name AS name,
    m.product_description AS description,
    max(m.product_price) AS price,
    max(m.product_quantity) AS quantity,
    max(s.id) AS supplier_id,
    max(pc.id) AS category_id,
    max(ppc.id) AS pet_category_id,
    max(pb.id) AS brand_id,
    max(pcl.id) AS color_id,
    max(pm.id) AS material_id,
    max(psz.id) AS size_id,
    max(m.product_weight) AS weight,
    max(m.product_rating) AS rating,
    max(m.product_reviews) AS reviews,
    m.product_release_date AS release_date,
    m.product_expiry_date AS expiry_date
  FROM clickhouse.spark.stg_mock_data m
  JOIN clickhouse.spark.product_suppliers s ON m.supplier_email = s.email
  JOIN clickhouse.spark.product_categories pc ON m.product_category = pc.name
  JOIN clickhouse.spark.product_pet_categories ppc ON m.pet_category = ppc.name
  JOIN clickhouse.spark.product_brands pb ON m.product_brand = pb.name
  JOIN clickhouse.spark.product_colors pcl ON m.product_color = pcl.name
  JOIN clickhouse.spark.product_materials pm ON m.product_material = pm.name
  JOIN clickhouse.spark.product_sizes psz ON m.product_size = psz.name
  WHERE m.product_name IS NOT NULL
    AND m.product_description IS NOT NULL
    AND m.product_release_date IS NOT NULL
    AND m.product_expiry_date IS NOT NULL
  GROUP BY
    m.product_name,
    m.product_description,
    m.product_release_date,
    m.product_expiry_date
)
SELECT
  CAST(row_number() OVER (ORDER BY name, description, release_date, expiry_date) AS INTEGER) AS id,
  name,
  description,
  price,
  quantity,
  supplier_id,
  category_id,
  pet_category_id,
  brand_id,
  color_id,
  material_id,
  size_id,
  weight,
  rating,
  reviews,
  release_date,
  expiry_date
FROM product_rows;

CREATE TABLE clickhouse.spark.stores(
  id        INTEGER,
  name      VARCHAR,
  location  VARCHAR,
  city      VARCHAR,
  state     VARCHAR,
  country   VARCHAR,
  email     VARCHAR,
  phone     VARCHAR
)
WITH (
  engine = 'MergeTree'
);

INSERT INTO clickhouse.spark.stores
SELECT
  CAST(row_number() OVER (ORDER BY store_email) AS INTEGER) AS id,
  arbitrary(store_name) AS name,
  arbitrary(store_location) AS location,
  arbitrary(store_city) AS city,
  arbitrary(store_state) AS state,
  arbitrary(store_country) AS country,
  store_email AS email,
  arbitrary(store_phone) AS phone
FROM clickhouse.spark.stg_mock_data
WHERE store_email IS NOT NULL
GROUP BY store_email;

CREATE TABLE clickhouse.spark.sales(
  id           INTEGER,
  customer_id  INTEGER,
  date         DATE,
  seller_id    INTEGER,
  store_id     INTEGER,
  product_id   INTEGER,
  quantity     INTEGER,
  total_price  DECIMAL(10, 2)
)
WITH (
  engine = 'MergeTree'
);

INSERT INTO clickhouse.spark.sales
WITH sale_rows AS (
  SELECT DISTINCT
    c.id AS customer_id,
    m.sale_date AS date,
    sl.id AS seller_id,
    st.id AS store_id,
    p.id AS product_id,
    m.sale_quantity AS quantity,
    m.sale_total_price AS total_price
  FROM clickhouse.spark.stg_mock_data m
  JOIN clickhouse.spark.customers c ON m.customer_email = c.email
  JOIN clickhouse.spark.sellers sl ON m.seller_email = sl.email
  JOIN clickhouse.spark.stores st ON m.store_email = st.email
  JOIN clickhouse.spark.products p
    ON m.product_name = p.name
    AND m.product_description = p.description
    AND m.product_release_date = p.release_date
    AND m.product_expiry_date = p.expiry_date
  WHERE m.sale_date IS NOT NULL
)
SELECT
  CAST(row_number() OVER (ORDER BY date, customer_id, seller_id, store_id, product_id, quantity, total_price) AS INTEGER) AS id,
  customer_id,
  date,
  seller_id,
  store_id,
  product_id,
  quantity,
  total_price
FROM sale_rows;
