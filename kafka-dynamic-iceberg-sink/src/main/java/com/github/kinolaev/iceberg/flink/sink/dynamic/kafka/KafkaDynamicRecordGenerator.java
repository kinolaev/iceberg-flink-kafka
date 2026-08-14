package com.github.kinolaev.iceberg.flink.sink.dynamic.kafka;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.util.Collector;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.sink.AvroGenericRecordToRowDataMapper;
import org.apache.iceberg.flink.sink.dynamic.DynamicRecord;
import org.apache.iceberg.flink.sink.dynamic.DynamicRecordGenerator;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class KafkaDynamicRecordGenerator
    implements DynamicRecordGenerator<ConsumerRecord<byte[], byte[]>> {
  private static final String KAFKA_PREFIX = "kafka.";

  private final Map<String, String> props;

  private RouteConfig routeConfig;
  private LoadingCache<String, TableConfig> tableConfigCache;
  private LoadingCache<org.apache.avro.Schema, Set<String>> idColumnsCache;
  private LoadingCache<org.apache.avro.Schema, AvroGenericRecordToRowDataMapper> mapperCache;
  private KafkaAvroDeserializer deserializer;

  public KafkaDynamicRecordGenerator(Map<String, String> props) {
    this.props = props;
  }

  @Override
  public void open(OpenContext openContext) {
    routeConfig = new RouteConfig(props);
    tableConfigCache =
        CacheBuilder.newBuilder()
            .expireAfterAccess(Duration.ofHours(1))
            .build(new TableConfigCacheLoader(props));
    idColumnsCache = CacheBuilder.newBuilder().weakKeys().build(new IdColumnsCacheLoader());
    mapperCache = CacheBuilder.newBuilder().weakKeys().build(new RowDataMapperCacheLoader());
    deserializer =
        new KafkaAvroDeserializer(PropertyUtil.propertiesWithPrefix(props, KAFKA_PREFIX));
  }

  @Override
  public void generate(ConsumerRecord<byte[], byte[]> record, Collector<DynamicRecord> out)
      throws Exception {
    GenericRecord key =
        routeConfig.needsKey()
            ? deserializer.deserialize(record.topic(), true, record.headers(), record.value())
            : null;
    GenericRecord value =
        deserializer.deserialize(record.topic(), false, record.headers(), record.value());

    String tableName = routeConfig.getTableName(record.topic(), record.headers(), key, value);
    TableConfig tableConfig = tableConfigCache.get(tableName);
    Set<String> idColumns = tableConfig.idColumns();
    if (idColumns == null) {
      org.apache.avro.Schema keySchema =
          key == null
              ? deserializer.getSchema(record.topic(), true, record.headers(), record.key())
              : key.getSchema();
      idColumns = idColumnsCache.get(keySchema);
    }
    Schema schema = tableConfig.schemaCache().get(value.getSchema()).get(idColumns);

    DynamicRecord dynamicRecord =
        new DynamicRecord(
            TableIdentifier.parse(tableName),
            tableConfig.commitBranch(),
            schema,
            mapperCache.get(value.getSchema()).map(value),
            tableConfig.specCache().get(schema),
            tableConfig.distributionMode(),
            tableConfig.writeParallelism());
    if (tableConfig.upsertModeEnabled()) {
      dynamicRecord.setUpsertMode(true);
      dynamicRecord.setEqualityFields(
          Stream.concat(
                  dynamicRecord.schema().identifierFieldIds().stream(),
                  dynamicRecord.spec().fields().stream().map(PartitionField::sourceId))
              .map(dynamicRecord.schema()::findColumnName)
              .collect(Collectors.toCollection(LinkedHashSet::new)));
    }
    out.collect(dynamicRecord);
  }

  private static class IdColumnsCacheLoader
      extends CacheLoader<org.apache.avro.Schema, Set<String>> {
    @Override
    public @Nonnull Set<String> load(@Nonnull org.apache.avro.Schema keySchema) {
      return keySchema.getFields().stream()
          .map(org.apache.avro.Schema.Field::name)
          .collect(Collectors.toCollection(LinkedHashSet::new));
    }
  }

  private static class RowDataMapperCacheLoader
      extends CacheLoader<org.apache.avro.Schema, AvroGenericRecordToRowDataMapper> {
    @Override
    public @Nonnull AvroGenericRecordToRowDataMapper load(
        @Nonnull org.apache.avro.Schema avroSchema) {
      return AvroGenericRecordToRowDataMapper.forAvroSchema(avroSchema);
    }
  }
}
