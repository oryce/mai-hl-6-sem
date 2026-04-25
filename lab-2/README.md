```
cp .env.template .env
docker compose up -d
```

- Spark Master: http://localhost:8080
- Spark Worker: http://localhost:8081
- ClickHouse: http://localhost:8123
- Дефолтные username/password для всех: `spark`

```
docker compose exec \
  -e JDBC_URL=jdbc:postgresql://postgres:5432/spark \
  -e JDBC_USER=spark \
  -e JDBC_PASSWORD=spark \
  -e JDBC_DRIVER=org.postgresql.Driver \
  spark-master /opt/spark/bin/spark-submit /jobs/00-normalize.py
```

```
docker compose exec \
  -e SRC_JDBC_URL=jdbc:postgresql://postgres:5432/spark \
  -e SRC_JDBC_USER=spark \
  -e SRC_JDBC_PASSWORD=spark \
  -e SRC_JDBC_DRIVER=org.postgresql.Driver \
  -e DST_JDBC_URL=jdbc:clickhouse:http://clickhouse:8123/spark \
  -e DST_JDBC_USER=spark \
  -e DST_JDBC_PASSWORD=spark \
  -e DST_JDBC_DRIVER=com.clickhouse.jdbc.ClickHouseDriver \
  spark-master /opt/spark/bin/spark-submit /jobs/10-create-marts.py
```
