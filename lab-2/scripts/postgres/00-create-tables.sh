#!/bin/sh

set -eu

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
EOSQL
