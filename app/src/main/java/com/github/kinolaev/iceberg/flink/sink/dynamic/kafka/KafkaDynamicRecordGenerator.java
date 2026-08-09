package com.github.kinolaev.iceberg.flink.sink.dynamic.kafka;

import com.google.common.base.Splitter;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.sink.AvroGenericRecordToRowDataMapper;
import org.apache.iceberg.flink.sink.dynamic.DynamicRecord;
import org.apache.iceberg.flink.sink.dynamic.DynamicRecordGenerator;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class KafkaDynamicRecordGenerator
    implements DynamicRecordGenerator<ConsumerRecord<byte[], byte[]>> {
  private static final String SCHEMA_REGISTRY_PROP_PREFIX = "schema.registry.";
  private static final String TABLE_PROP_PREFIX = "iceberg.table.";
  private static final String TABLES_DEFAULT_COMMIT_BRANCH = "iceberg.tables.default-commit-branch";
  private static final String TABLES_DEFAULT_PARTITION_BY = "iceberg.tables.default-partition-by";
  private static final String TABLE_PARTITION_BY_PROP_SUFFIX = ".partition-by";
  private static final String COMMA_NO_PARENS_REGEX = ",(?![^()]*+\\))";
  private static final Pattern TRANSFORM_REGEX = Pattern.compile("(\\w+)\\((.+)\\)");

  private final Map<String, String> props;

  private String defaultCommitBranch;
  private String defaultPartitionBy;
  private KafkaAvroDeserializer deserializer;
  private LoadingCache<org.apache.avro.Schema, LoadingCache<org.apache.avro.Schema, Schema>>
      schemaCache;
  private LoadingCache<org.apache.avro.Schema, AvroGenericRecordToRowDataMapper> mapperCache;

  public KafkaDynamicRecordGenerator(Map<String, String> props) {
    this.props = props;
  }

  @Override
  public void open(OpenContext openContext) {
    defaultCommitBranch = props.getOrDefault(TABLES_DEFAULT_COMMIT_BRANCH, SnapshotRef.MAIN_BRANCH);
    defaultPartitionBy = props.get(TABLES_DEFAULT_PARTITION_BY);
    deserializer =
        new KafkaAvroDeserializer(
            PropertyUtil.filterProperties(
                props, key -> key.startsWith(SCHEMA_REGISTRY_PROP_PREFIX)));
    schemaCache = CacheBuilder.newBuilder().weakKeys().build(new SchemasCacheLoader());
    mapperCache = CacheBuilder.newBuilder().weakKeys().build(new MapperCacheLoader());
  }

  @Override
  public void generate(ConsumerRecord<byte[], byte[]> record, Collector<DynamicRecord> out)
      throws Exception {
    String tableName = record.topic().substring(record.topic().indexOf('.') + 1);
    TableIdentifier tableId = TableIdentifier.parse(tableName);

    GenericRecord key =
        deserializer.deserialize(record.topic(), true, record.headers(), record.key());
    GenericRecord value =
        deserializer.deserialize(record.topic(), false, record.headers(), record.value());
    Schema schema = schemaCache.get(value.getSchema()).get(key.getSchema());
    RowData rowData = mapperCache.get(value.getSchema()).map(value);

    String partitionBy =
        props.getOrDefault(
            TABLE_PROP_PREFIX + tableName + TABLE_PARTITION_BY_PROP_SUFFIX, defaultPartitionBy);
    PartitionSpec spec =
        createPartitionSpec(
            schema,
            partitionBy == null
                ? List.of()
                : Arrays.stream(partitionBy.split(COMMA_NO_PARENS_REGEX))
                    .map(String::trim)
                    .collect(Collectors.toList()));
    DynamicRecord dynamicRecord =
        new DynamicRecord(tableId, defaultCommitBranch, schema, rowData, spec);
    dynamicRecord.setUpsertMode(true);
    dynamicRecord.setEqualityFields(
        Stream.concat(
                key.getSchema().getFields().stream().map(org.apache.avro.Schema.Field::name),
                spec.fields().stream().map(PartitionField::sourceId).map(schema::findColumnName))
            .collect(Collectors.toSet()));
    out.collect(dynamicRecord);
  }

  private static class SchemasCacheLoader
      extends CacheLoader<org.apache.avro.Schema, LoadingCache<org.apache.avro.Schema, Schema>> {
    @Override
    public LoadingCache<org.apache.avro.Schema, Schema> load(org.apache.avro.Schema valueSchema)
        throws Exception {
      return CacheBuilder.newBuilder().weakKeys().build(new SchemaCacheLoader(valueSchema));
    }
  }

  private static class SchemaCacheLoader extends CacheLoader<org.apache.avro.Schema, Schema> {
    private org.apache.avro.Schema valueSchema;

    SchemaCacheLoader(org.apache.avro.Schema valueSchema) {
      this.valueSchema = valueSchema;
    }

    @Override
    public Schema load(org.apache.avro.Schema keySchema) throws Exception {
      return convertSchema(valueSchema, keySchema);
    }
  }

  private static class MapperCacheLoader
      extends CacheLoader<org.apache.avro.Schema, AvroGenericRecordToRowDataMapper> {
    @Override
    public AvroGenericRecordToRowDataMapper load(org.apache.avro.Schema avroSchema)
        throws Exception {
      return AvroGenericRecordToRowDataMapper.forAvroSchema(avroSchema);
    }
  }

  private static Schema convertSchema(
      org.apache.avro.Schema valueSchema, org.apache.avro.Schema keySchema) {
    final Set<Integer> identifierFieldIds = new HashSet<>(keySchema.getFields().size());
    final List<Types.NestedField> fields = new ArrayList<>(valueSchema.getFields().size());
    AvroSchemaUtil.convert(valueSchema)
        .asNestedType()
        .asStructType()
        .fields()
        .forEach(
            field -> {
              if (keySchema.getField(field.name()) == null) {
                fields.add(field.asOptional());
              } else {
                identifierFieldIds.add(field.fieldId());
                fields.add(field);
              }
            });
    return new Schema(fields, identifierFieldIds);
  }

  // https://github.com/apache/iceberg/blob/apache-iceberg-1.11.0/kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/data/SchemaUtils.java#L153-L210
  private static PartitionSpec createPartitionSpec(
      org.apache.iceberg.Schema schema, List<String> partitionBy) {
    if (partitionBy.isEmpty()) {
      return PartitionSpec.unpartitioned();
    }

    PartitionSpec.Builder specBuilder = PartitionSpec.builderFor(schema);
    partitionBy.forEach(
        partitionField -> {
          Matcher matcher = TRANSFORM_REGEX.matcher(partitionField);
          if (matcher.matches()) {
            String transform = matcher.group(1);
            switch (transform) {
              case "year":
              case "years":
                specBuilder.year(matcher.group(2));
                break;
              case "month":
              case "months":
                specBuilder.month(matcher.group(2));
                break;
              case "day":
              case "days":
                specBuilder.day(matcher.group(2));
                break;
              case "hour":
              case "hours":
                specBuilder.hour(matcher.group(2));
                break;
              case "bucket":
                {
                  Pair<String, Integer> args = transformArgPair(matcher.group(2));
                  specBuilder.bucket(args.first(), args.second());
                  break;
                }
              case "truncate":
                {
                  Pair<String, Integer> args = transformArgPair(matcher.group(2));
                  specBuilder.truncate(args.first(), args.second());
                  break;
                }
              default:
                throw new UnsupportedOperationException("Unsupported transform: " + transform);
            }
          } else {
            specBuilder.identity(partitionField);
          }
        });
    return specBuilder.build();
  }

  private static Pair<String, Integer> transformArgPair(String argsStr) {
    List<String> parts = Splitter.on(',').splitToList(argsStr);
    if (parts.size() != 2) {
      throw new IllegalArgumentException("Invalid argument " + argsStr + ", should have 2 parts");
    }
    return Pair.of(parts.get(0).trim(), Integer.parseInt(parts.get(1).trim()));
  }
}
