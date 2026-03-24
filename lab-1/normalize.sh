#!/bin/sh

postgres() {
  psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" "${@}"
}

postgres <<-'EOSQL'
CREATE TABLE customers(
  id          SERIAL PRIMARY KEY,
  first_name  TEXT NOT NULL,
  last_name   TEXT NOT NULL,
  age         SMALLINT NOT NULL,
  email       TEXT UNIQUE,
  country     TEXT NOT NULL,
  postal_code TEXT
);

CREATE TABLE pet_breeds(
  id      SERIAL PRIMARY KEY,
  name    TEXT UNIQUE NOT NULL
);

CREATE TABLE pet_types(
  id    SERIAL PRIMARY KEY,
  name  TEXT UNIQUE NOT NULL
);

CREATE TABLE customer_pets(
  id          SERIAL PRIMARY KEY,
  customer_id INTEGER NOT NULL REFERENCES customers(id),
  name        TEXT NOT NULL,
  breed_id    INTEGER NOT NULL REFERENCES pet_breeds(id),
  type_id     INTEGER NOT NULL REFERENCES pet_types(id)
);

CREATE TABLE sellers(
  id          SERIAL PRIMARY KEY,
  first_name  TEXT NOT NULL,
  last_name   TEXT NOT NULL,
  email       TEXT UNIQUE,
  country     TEXT NOT NULL,
  postal_code TEXT
);

CREATE TABLE product_brands(
  id    SERIAL PRIMARY KEY,
  name  TEXT UNIQUE NOT NULL
);

CREATE TABLE product_categories(
  id    SERIAL PRIMARY KEY,
  name  TEXT UNIQUE NOT NULL
);

CREATE TABLE product_colors(
  id    SERIAL PRIMARY KEY,
  name  TEXT UNIQUE NOT NULL
);

CREATE TABLE product_materials(
  id    SERIAL PRIMARY KEY,
  name  TEXT UNIQUE NOT NULL
);

CREATE TABLE product_pet_categories(
  id    SERIAL PRIMARY KEY,
  name  TEXT UNIQUE NOT NULL
);

CREATE TABLE product_sizes(
  id    SERIAL PRIMARY KEY,
  name  TEXT UNIQUE NOT NULL
);

CREATE TABLE product_suppliers(
  id      SERIAL PRIMARY KEY,
  name    TEXT NOT NULL,
  contact TEXT NOT NULL,
  email   TEXT UNIQUE,
  phone   TEXT NOT NULL,
  address TEXT NOT NULL,
  city    TEXT NOT NULL,
  country TEXT NOT NULL
);

CREATE TABLE products(
  id                SERIAL PRIMARY KEY,

  name              TEXT NOT NULL,
  description       TEXT NOT NULL,
  price             NUMERIC(10, 2) NOT NULL,
  
  quantity          INTEGER NOT NULL,
  supplier_id       INTEGER NOT NULL REFERENCES product_suppliers(id),
  
  category_id       INTEGER NOT NULL REFERENCES product_categories(id),
  pet_category_id   INTEGER NOT NULL REFERENCES product_pet_categories(id),

  brand_id          INTEGER NOT NULL REFERENCES product_brands(id),
  color_id          INTEGER NOT NULL REFERENCES product_colors(id),
  material_id       INTEGER NOT NULL REFERENCES product_materials(id),
  size_id           INTEGER NOT NULL REFERENCES product_sizes(id),
  weight            NUMERIC(6, 2) NOT NULL,
  
  rating            NUMERIC(2, 1) NOT NULL,
  reviews           INTEGER NOT NULL,
  
  release_date      DATE NOT NULL,
  expiry_date       DATE NOT NULL
);

CREATE TABLE stores(
  id        SERIAL PRIMARY KEY,
  name      TEXT NOT NULL,
  location  TEXT NOT NULL,
  city      TEXT NOT NULL,
  state     TEXT,
  country   TEXT NOT NULL,
  email     TEXT UNIQUE,
  phone     TEXT NOT NULL
);

CREATE TABLE sales(
  id          SERIAL PRIMARY KEY,
  
  customer_id INTEGER NOT NULL REFERENCES customers(id),
  date        DATE NOT NULL,
  seller_id   INTEGER NOT NULL REFERENCES sellers(id),
  store_id    INTEGER NOT NULL REFERENCES stores(id),

  product_id  INTEGER NOT NULL REFERENCES products(id),
  quantity    INTEGER NOT NULL,
  total_price NUMERIC(10, 2) NOT NULL
);

INSERT INTO customers(
  first_name, 
  last_name, 
  age, 
  email, 
  country, 
  postal_code
)
SELECT DISTINCT
  customer_first_name,
  customer_last_name,
  customer_age,
  customer_email,
  customer_country,
  customer_postal_code
FROM mock_data;

INSERT INTO pet_breeds(name) SELECT DISTINCT customer_pet_breed FROM mock_data;
INSERT INTO pet_types(name) SELECT DISTINCT customer_pet_type FROM mock_data;

INSERT INTO customer_pets(customer_id, name, breed_id, type_id)
SELECT customers.id, customer_pet_name, pet_breeds.id, pet_types.id
FROM mock_data
JOIN customers ON mock_data.customer_email = customers.email
JOIN pet_breeds ON mock_data.customer_pet_breed = pet_breeds.name
JOIN pet_types ON mock_data.customer_pet_type = pet_types.name;

INSERT INTO sellers(
  first_name,
  last_name,
  email,
  country,
  postal_code
)
SELECT DISTINCT
  seller_first_name,
  seller_last_name,
  seller_email,
  seller_country,
  seller_postal_code
FROM mock_data;

INSERT INTO product_brands(name) SELECT DISTINCT product_brand FROM mock_data;
INSERT INTO product_categories(name) SELECT DISTINCT product_category FROM mock_data;
INSERT INTO product_colors(name) SELECT DISTINCT product_color FROM mock_data;
INSERT INTO product_materials(name) SELECT DISTINCT product_material FROM mock_data;
INSERT INTO product_pet_categories(name) SELECT DISTINCT pet_category FROM mock_data;
INSERT INTO product_sizes(name) SELECT DISTINCT product_size FROM mock_data;

INSERT INTO product_suppliers(
  name,
  contact,
  email,
  phone,
  address,
  city,
  country
)
SELECT DISTINCT
  supplier_name,
  supplier_contact,
  supplier_email,
  supplier_phone,
  supplier_address,
  supplier_city,
  supplier_country
FROM mock_data;

INSERT INTO products(
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
)
SELECT 
  product_name,
  product_description,
  product_price,
  product_quantity,
  product_suppliers.id,
  product_categories.id,
  product_pet_categories.id,
  product_brands.id,
  product_colors.id,
  product_materials.id,
  product_sizes.id,
  product_weight,
  product_rating,
  product_reviews,
  product_release_date,
  product_expiry_date
FROM mock_data
JOIN product_suppliers ON mock_data.supplier_email = product_suppliers.email
JOIN product_categories ON mock_data.product_category = product_categories.name
JOIN product_pet_categories ON mock_data.pet_category = product_pet_categories.name
JOIN product_brands ON mock_data.product_brand = product_brands.name
JOIN product_colors ON mock_data.product_color = product_colors.name
JOIN product_materials ON mock_data.product_material = product_materials.name
JOIN product_sizes ON mock_data.product_size = product_sizes.name;

INSERT INTO stores(
  name,
  location,
  city,
  state,
  country,
  email,
  phone
)
SELECT DISTINCT
  store_name,
  store_location,
  store_city,
  store_state,
  store_country,
  store_email,
  store_phone
FROM mock_data;

INSERT INTO sales(
  customer_id,
  date,
  seller_id,
  store_id,
  product_id,
  quantity,
  total_price
)
SELECT 
  customers.id,
  sale_date,
  sellers.id,
  stores.id,
  products.id,
  sale_quantity,
  sale_total_price
FROM mock_data
JOIN customers ON mock_data.customer_email = customers.email
JOIN sellers ON mock_data.seller_email = sellers.email
JOIN products ON mock_data.product_name = products.name 
  AND mock_data.product_description = products.description
  AND mock_data.product_release_date = products.release_date
  AND mock_data.product_expiry_date = products.expiry_date
JOIN stores ON mock_data.store_email = stores.email;
EOSQL
