# Flink Kafka Dynamic Iceberg Sink Job

The application ingests Avro-encoded data from Kafka topics using the official [Flink Dynamic Iceberg Sink](https://iceberg.apache.org/docs/latest/flink-writes/#flink-dynamic-iceberg-sink) that supports upsert mode, automatic table creation and schema evolution.
Avro to Iceberg conversion is implemented using `AvroSchemaUtil` from `iceberg-core` and `AvroGenericRecordToRowDataMapper` from `iceberg-flink`.
Additionally, the application supports several [Debezium-specific types](kafka-dynamic-iceberg-sink/src/main/java/com/github/kinolaev/avro/Converters.java).

## Setup

The project contains a `compose` environment with `postgres`, `debezium-server`, `redpanda`, a `flink` session cluster, `rustfs`, and `trino`.
To start a `rustfs`-managed Iceberg REST Catalog, run:
```bash
docker compose up -d rustfs
docker compose exec rustfs curl \
  -X PUT http://localhost:9000/warehouse \
  --aws-sigv4 aws:amz:us-east-1:s3 --user rustfs:rustfs
docker compose exec rustfs curl \
  -X PUT http://localhost:9000/iceberg/v1/buckets/warehouse \
  --aws-sigv4 aws:amz:us-east-1:s3 --user rustfs:rustfs
```
Then build the application and run the other services:
```bash
./gradlew :kafka-dynamic-iceberg-sink:shadowJar
docker compose up
```
Alternatively, you can use the latest [prebuilt container image](https://github.com/kinolaev/iceberg-flink-kafka/pkgs/container/iceberg-flink-kafka):
```bash
docker compose -f compose-latest.yaml up
```

## Usage

To launch the application in the `flink` cluster, run:
```bash
docker compose exec jobmanager flink run -sae \
  /opt/flink/usrlib/kafka-dynamic-iceberg-sink-all.jar \
  /opt/flink/usrconf/config.properties \
  /opt/flink/usrconf/secret.properties
```
The application supports most of the configuration properties from Iceberg Kafka Connect to ease migration.
Additionally, it extends the routing configuration and supports properties specific to the Flink Dynamic Iceberg Sink.
See the [compose/flink/usrconf](compose/flink/usrconf) directory for the full list of supported properties.
Secret properties can be specified in a separate file as shown in the command above.

## Verification

Several tables are automatically created and populated during setup.
You can find them in the [compose/postgres/initdb.d](compose/postgres/initdb.d) directory.
Use `trino` to inspect the ingested data:
```bash
docker compose exec trino trino --execute 'select * from iceberg.public.logicaltypes'
docker compose exec trino trino --execute 'select * from iceberg.public.partitioned'
docker compose exec trino trino --execute 'select * from iceberg.public.sort_order'
```

## Cleanup

```bash
docker compose down -v
```
