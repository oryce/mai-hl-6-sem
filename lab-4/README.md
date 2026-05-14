# BigDataTrino

Лабораторная работа N4: ETL на Trino из двух однотипных raw-источников в модель звезда/снежинка и 6 отчетных таблиц в ClickHouse.

## Состав

- `source/` - 10 CSV-файлов по 1000 строк.
- `docker-compose.yml` - PostgreSQL, ClickHouse, Trino.
- `scripts/postgres/00-create-and-import.sh` - создает `mock_data` в PostgreSQL и загружает `MOCK_DATA (5).csv` ... `MOCK_DATA (9).csv`.
- `scripts/clickhouse/00-create-and-import.sh` - создает `mock_data` в ClickHouse и загружает `MOCK_DATA.csv`, `MOCK_DATA (1).csv` ... `MOCK_DATA (4).csv`.
- `scripts/trino/00-create-model.sql` - объединяет источники через Trino и создает staging, измерения и факт продаж в ClickHouse.
- `scripts/trino/10-create-marts.sql` - создает 6 витрин в ClickHouse.

## Запуск

```sh
cd lab-4
cp .env.template .env
podman compose up -d
```

Дождаться запуска контейнеров и выполнить Trino-скрипты:

```sh
podman compose exec -T trino trino --file /scripts/00-create-model.sql
podman compose exec -T trino trino --file /scripts/10-create-marts.sql
```

Если нужно пересоздать базы и заново импортировать CSV:

```sh
podman compose down -v
podman compose up -d
```

## Проверка

Проверить количество строк в исходниках:

```sh
podman compose exec -T postgres \
  psql -U spark -d spark -c 'SELECT count(*) AS postgres_rows FROM mock_data;'

podman compose exec -T clickhouse \
  clickhouse-client --user spark --password spark --database spark \
  --query 'SELECT count() AS clickhouse_rows FROM mock_data'
```

Проверить модель и витрины в ClickHouse:

```sh
podman compose exec -T clickhouse \
  clickhouse-client --user spark --password spark --database spark \
  --query "
    SELECT *
    FROM (
      SELECT 'stg_mock_data' AS table_name, count() AS rows_count FROM stg_mock_data
      UNION ALL SELECT 'sales', count() FROM sales
      UNION ALL SELECT 'product_sales', count() FROM product_sales
      UNION ALL SELECT 'customer_sales', count() FROM customer_sales
      UNION ALL SELECT 'time_sales', count() FROM time_sales
      UNION ALL SELECT 'store_sales', count() FROM store_sales
      UNION ALL SELECT 'supplier_sales', count() FROM supplier_sales
      UNION ALL SELECT 'product_quality', count() FROM product_quality
    )
    ORDER BY table_name"
```

Ожидаемые ключевые значения после полного запуска:

- PostgreSQL `mock_data`: `5000`
- ClickHouse raw `mock_data`: `5000`
- ClickHouse `stg_mock_data`: `10000`
- ClickHouse `sales`: `10000`
- `time_sales`: `12`

## Витрины

- `product_sales` - продажи по продуктам: топ-10 по количеству, выручка по категориям, рейтинг и отзывы.
- `customer_sales` - продажи по клиентам: топ-10 клиентов, страны, средний чек.
- `time_sales` - продажи по времени: месячные и годовые тренды, сравнение с предыдущим месяцем, средний заказ.
- `store_sales` - продажи по магазинам: топ-5 магазинов, города/страны, средний чек.
- `supplier_sales` - продажи по поставщикам: топ-5 поставщиков, средняя цена товаров, страны поставщиков.
- `product_quality` - качество продукции: лучшие/худшие рейтинги, корреляция рейтинга и продаж, количество отзывов.

## Подключения

- Trino Web UI: http://localhost:8080
- ClickHouse HTTP: http://localhost:8123
- Логин/пароль по умолчанию для PostgreSQL и ClickHouse: `spark` / `spark`
