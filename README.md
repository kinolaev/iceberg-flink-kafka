# Flink Kafka Dynamic Iceberg Sink Job

## Setup

```bash
docker compose up -d rustfs
docker compose exec rustfs curl \
  -X PUT http://localhost:9000/warehouse \
  --aws-sigv4 aws:amz:us-east-1:s3 --user rustfs:rustfs
docker compose exec rustfs curl \
  -X PUT http://localhost:9000/iceberg/v1/buckets/warehouse \
  --aws-sigv4 aws:amz:us-east-1:s3 --user rustfs:rustfs
./gradlew :kafka-dynamic-iceberg-sink:shadowJar
docker compose up
```

## Usage

```bash
docker compose exec jobmanager flink run -sae \
  /opt/flink/usrlib/kafka-dynamic-iceberg-sink-all.jar \
  /opt/flink/usrconf/config.properties \
  /opt/flink/usrconf/secret.properties
```

## Verification

```bash
docker compose exec trino trino --execute 'select * from iceberg.public.logicaltypes'
docker compose exec trino trino --execute 'select * from iceberg.public.partitioned'
docker compose exec trino trino --execute 'select * from iceberg.public.sort_order'
```

## Cleanup

```bash
docker compose down -v
```
