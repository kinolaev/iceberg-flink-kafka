package com.github.kinolaev.iceberg.flink.sink.dynamic.kafka;

import com.google.common.base.Splitter;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.apache.iceberg.DistributionMode;
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
import org.apache.kafka.common.header.Headers;

public class KafkaDynamicRecordGenerator
    implements DynamicRecordGenerator<ConsumerRecord<byte[], byte[]>> {
  private static final String SCHEMA_REGISTRY_PREFIX = "schema.registry.";

  private static final String TABLES_ROUTE_FIELD = "iceberg.tables.route-field";
  private static final String TABLES_ROUTE_REGEXP_PROP = "iceberg.tables.route-regexp";
  private static final String TABLES_ROUTE_REGEXP_REPLACE_PROP =
      "iceberg.tables.route-regexp-replace";
  private static final String TABLES_ROUTE_FORCE_CASE_PROP = "iceberg.tables.route-force-case";
  private static final String TABLES_DEFAULT_COMMIT_BRANCH_PROP =
      "iceberg.tables.default-commit-branch";
  private static final String TABLES_DEFAULT_PARTITION_BY_PROP =
      "iceberg.tables.default-partition-by";
  private static final String TABLES_SCHEMA_FORCE_OPTIONAL_PROP =
      "iceberg.tables.schema-force-optional";
  private static final String TABLES_SCHEMA_FORCE_CASE_PROP = "iceberg.tables.schema-force-case";
  private static final String TABLES_UPSERT_MODE_ENABLED_PROP =
      "iceberg.tables.upsert-mode-enabled";
  private static final String TABLES_DISTRIBUTION_MODE_PROP = "iceberg.tables.distribution-mode";
  private static final String TABLES_WRITE_PARALLELISM_PROP = "iceberg.tables.write-parallelism";

  private static final String TABLE_COMMIT_BRANCH_PROP = "iceberg.table.%s.commit-branch";
  private static final String TABLE_PARTITION_BY_PROP = "iceberg.table.%s.partition-by";

  private static final String COMMA_NO_PARENS_REGEX = ",(?![^()]*+\\))";
  private static final Pattern TRANSFORM_REGEX = Pattern.compile("(\\w+)\\((.+)\\)");

  private final Map<String, String> props;

  private RouteConfig routeConfig;
  private String defaultCommitBranch;
  private List<String> defaultPartitionBy;
  private SchemaConfig schemaConfig;
  private boolean upsertModeEnabled;
  private DistributionMode distributionMode;
  private int writeParallelism;
  private KafkaAvroDeserializer deserializer;
  private LoadingCache<org.apache.avro.Schema, LoadingCache<org.apache.avro.Schema, Schema>>
      schemaCache;
  private LoadingCache<org.apache.avro.Schema, AvroGenericRecordToRowDataMapper> mapperCache;

  public KafkaDynamicRecordGenerator(Map<String, String> props) {
    this.props = props;
  }

  @Override
  public void open(OpenContext openContext) {
    routeConfig = new RouteConfig(props);
    defaultCommitBranch =
        props.getOrDefault(TABLES_DEFAULT_COMMIT_BRANCH_PROP, SnapshotRef.MAIN_BRANCH);
    defaultPartitionBy =
        Optional.ofNullable(props.get(TABLES_DEFAULT_PARTITION_BY_PROP))
            .map(KafkaDynamicRecordGenerator::parsePartitionBy)
            .orElse(List.of());
    schemaConfig = new SchemaConfig(props);
    upsertModeEnabled =
        Boolean.parseBoolean(props.getOrDefault(TABLES_UPSERT_MODE_ENABLED_PROP, "false"));
    distributionMode =
        Optional.ofNullable(props.get(TABLES_DISTRIBUTION_MODE_PROP))
            .map(DistributionMode::fromName)
            .orElse(null);
    writeParallelism = Integer.parseInt(props.getOrDefault(TABLES_WRITE_PARALLELISM_PROP, "-1"));
    deserializer =
        new KafkaAvroDeserializer(
            PropertyUtil.filterProperties(props, key -> key.startsWith(SCHEMA_REGISTRY_PREFIX)));
    schemaCache = CacheBuilder.newBuilder().weakKeys().build(new SchemasCacheLoader(schemaConfig));
    mapperCache = CacheBuilder.newBuilder().weakKeys().build(new MapperCacheLoader());
  }

  @Override
  public void generate(ConsumerRecord<byte[], byte[]> record, Collector<DynamicRecord> out)
      throws Exception {
    GenericRecord key =
        routeConfig.field().startsWith("key.")
            ? deserializer.deserialize(record.topic(), true, record.headers(), record.value())
            : null;
    org.apache.avro.Schema keySchema =
        key == null
            ? deserializer.getSchema(record.topic(), true, record.headers(), record.key())
            : key.getSchema();
    GenericRecord value =
        deserializer.deserialize(record.topic(), false, record.headers(), record.value());

    String tableName = getTableName(routeConfig, record.topic(), record.headers(), key, value);

    String branch =
        props.getOrDefault(TABLE_COMMIT_BRANCH_PROP.formatted(tableName), defaultCommitBranch);

    Schema schema = schemaCache.get(value.getSchema()).get(keySchema);
    RowData rowData = mapperCache.get(value.getSchema()).map(value);

    List<String> partitionBy =
        Optional.ofNullable(props.get(TABLE_PARTITION_BY_PROP.formatted(tableName)))
            .map(KafkaDynamicRecordGenerator::parsePartitionBy)
            .orElse(defaultPartitionBy);
    PartitionSpec spec = createPartitionSpec(schema, partitionBy);

    TableIdentifier tableId = TableIdentifier.parse(tableName);
    DynamicRecord dynamicRecord =
        new DynamicRecord(
            tableId, branch, schema, rowData, spec, distributionMode, writeParallelism);
    if (upsertModeEnabled) {
      dynamicRecord.setUpsertMode(true);
      dynamicRecord.setEqualityFields(
          Stream.concat(
                  schema.identifierFieldIds().stream().map(schema::findColumnName),
                  spec.fields().stream().map(PartitionField::sourceId).map(schema::findColumnName))
              .collect(Collectors.toCollection(LinkedHashSet::new)));
    }
    out.collect(dynamicRecord);
  }

  private static class SchemasCacheLoader
      extends CacheLoader<org.apache.avro.Schema, LoadingCache<org.apache.avro.Schema, Schema>> {
    private SchemaConfig config;

    SchemasCacheLoader(SchemaConfig config) {
      this.config = config;
    }

    @Override
    public LoadingCache<org.apache.avro.Schema, Schema> load(org.apache.avro.Schema valueSchema)
        throws Exception {
      return CacheBuilder.newBuilder().weakKeys().build(new SchemaCacheLoader(config, valueSchema));
    }
  }

  private static class SchemaCacheLoader extends CacheLoader<org.apache.avro.Schema, Schema> {
    private SchemaConfig config;
    private org.apache.avro.Schema valueSchema;

    SchemaCacheLoader(SchemaConfig config, org.apache.avro.Schema valueSchema) {
      this.config = config;
      this.valueSchema = valueSchema;
    }

    @Override
    public Schema load(org.apache.avro.Schema keySchema) throws Exception {
      return convertSchema(config, valueSchema, keySchema);
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

  static enum ForceCase {
    UPPER,
    LOWER;
  }

  static record RouteConfig(
      String field, String regexp, String regexpReplace, ForceCase forceCase) {
    RouteConfig(Map<String, String> props) {
      this(
          props.getOrDefault(TABLES_ROUTE_FIELD, "topic"),
          props.get(TABLES_ROUTE_REGEXP_PROP),
          props.getOrDefault(TABLES_ROUTE_REGEXP_REPLACE_PROP, ""),
          Optional.ofNullable(props.get(TABLES_ROUTE_FORCE_CASE_PROP))
              .map(String::toUpperCase)
              .map(ForceCase::valueOf)
              .orElse(null));
    }
  }

  static String getTableName(
      RouteConfig config, String topic, Headers headers, GenericRecord key, GenericRecord value) {
    String[] path = config.field().split("\\.", 2);
    String tableName;
    try {
      tableName =
          switch (path[0]) {
            case "topic" -> topic;
            case "headers" -> new String(headers.lastHeader(path[1]).value());
            case "key" -> (String) key.get(path[1]);
            case "value" -> (String) value.get(path[1]);
            default -> throw new IllegalArgumentException("Unexpected record key");
          };
    } catch (Exception cause) {
      throw new IllegalArgumentException(TABLES_ROUTE_FIELD + "=" + config.field(), cause);
    }
    if (config.regexp() != null) {
      tableName = tableName.replaceAll(config.regexp(), config.regexpReplace());
    }
    return switch (config.forceCase()) {
      case null -> tableName;
      case UPPER -> tableName.toUpperCase();
      case LOWER -> tableName.toLowerCase();
    };
  }

  static record SchemaConfig(boolean forceOptional, ForceCase forceCase) {
    SchemaConfig(Map<String, String> props) {
      this(
          Boolean.parseBoolean(props.getOrDefault(TABLES_SCHEMA_FORCE_OPTIONAL_PROP, "false")),
          Optional.ofNullable(props.get(TABLES_SCHEMA_FORCE_CASE_PROP))
              .map(String::toUpperCase)
              .map(ForceCase::valueOf)
              .orElse(null));
    }
  }

  static Schema convertSchema(
      SchemaConfig config, org.apache.avro.Schema valueSchema, org.apache.avro.Schema keySchema) {
    Map<String, Integer> idToName = new HashMap<>(keySchema.getFields().size());
    List<Types.NestedField> fields = new ArrayList<>(valueSchema.getFields().size());
    Types.StructType struct = AvroSchemaUtil.convert(valueSchema).asNestedType().asStructType();
    for (Types.NestedField field : struct.fields()) {
      boolean isKey = keySchema.getField(field.name()) != null;
      if (isKey) idToName.put(field.name(), field.fieldId());
      fields.add(convertField(config, isKey, field));
    }
    Set<Integer> identifierFieldIds = new LinkedHashSet<>(idToName.size());
    for (org.apache.avro.Schema.Field field : keySchema.getFields()) {
      Integer id = idToName.get(field.name());
      if (id != null) identifierFieldIds.add(id);
    }
    return new Schema(fields, identifierFieldIds);
  }

  private static Types.NestedField convertField(
      SchemaConfig config, boolean isRequired, Types.NestedField field) {
    Types.NestedField newField = !isRequired && config.forceOptional() ? field.asOptional() : field;
    return switch (config.forceCase()) {
      case null -> newField;
      case UPPER ->
          Types.NestedField.from(newField).withName(newField.name().toUpperCase()).build();
      case LOWER ->
          Types.NestedField.from(newField).withName(newField.name().toLowerCase()).build();
    };
  }

  private static List<String> parsePartitionBy(String partitionBy) {
    return Arrays.stream(partitionBy.split(COMMA_NO_PARENS_REGEX)).map(String::trim).toList();
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
