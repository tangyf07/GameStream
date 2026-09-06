# Flink connector JARs (G2)

Mounted into JM/TM at `/opt/flink/usrlib` via `docker-compose.yml`.

| JAR | Role |
|-----|------|
| `flink-sql-connector-kafka-3.0.2-1.18.jar` | Kafka SQL source/sink |
| `flink-connector-jdbc-3.1.2-1.17.jar` | JDBC sink (Doris FE MySQL :9030) |
| `mysql-connector-j-8.0.33.jar` | MySQL driver for Doris |

If missing, download (prefer Maven Central / configured mirrors):

```bash
cd flink/jars
curl -fL -o flink-sql-connector-kafka-3.0.2-1.18.jar \
  https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.2-1.18/flink-sql-connector-kafka-3.0.2-1.18.jar
curl -fL -o flink-connector-jdbc-3.1.2-1.17.jar \
  https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.2-1.17/flink-connector-jdbc-3.1.2-1.17.jar
curl -fL -o mysql-connector-j-8.0.33.jar \
  https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar
```

Then `docker compose up -d --force-recreate jobmanager taskmanager`.
