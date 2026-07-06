# Iceberg Flink Kafka Dynamic Sink Job

```bash
docker compose up bucket-setup polaris-setup
docker compose up connect
docker compose exec connect curl -i -X POST -H "Accept:application/json" -H  "Content-Type:application/json" http://localhost:8083/connectors/ -d @register-postgres.json
docker compose up jobmanager taskmanager
```
