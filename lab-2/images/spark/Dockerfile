ARG SPARK_TAG

FROM spark:${SPARK_TAG}

USER root

ARG CLICKHOUSE_SPARK_VERSION
ARG CLICKHOUSE_RUNTIME_VERSION
ARG CLICKHOUSE_JDBC_VERSION
ARG POSTGRES_JDBC_VERSION

RUN \
  wget -O /opt/spark/jars/clickhouse-spark-runtime-${CLICKHOUSE_SPARK_VERSION}-${CLICKHOUSE_RUNTIME_VERSION}.jar \
    https://repo1.maven.org/maven2/com/clickhouse/spark/clickhouse-spark-runtime-${CLICKHOUSE_SPARK_VERSION}/${CLICKHOUSE_RUNTIME_VERSION}/clickhouse-spark-runtime-${CLICKHOUSE_SPARK_VERSION}-${CLICKHOUSE_RUNTIME_VERSION}.jar && \
  wget -O /opt/spark/jars/clickhouse-jdbc-${CLICKHOUSE_JDBC_VERSION}-all.jar \
    https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/${CLICKHOUSE_JDBC_VERSION}/clickhouse-jdbc-${CLICKHOUSE_JDBC_VERSION}-all.jar && \
  wget -O /opt/spark/jars/postgresql-${POSTGRES_JDBC_VERSION}.jar \
    https://jdbc.postgresql.org/download/postgresql-${POSTGRES_JDBC_VERSION}.jar

USER spark
