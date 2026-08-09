package com.github.kinolaev.iceberg.flink.sink.dynamic.kafka;

import com.github.kinolaev.iceberg.flink.sink.dynamic.kafka.connect.AddLogicalType;
import com.github.kinolaev.iceberg.flink.sink.dynamic.kafka.connect.Converter;
import com.github.kinolaev.iceberg.flink.sink.dynamic.kafka.connect.io.debezium.data.Uuid;
import com.github.kinolaev.iceberg.flink.sink.dynamic.kafka.connect.io.debezium.data.VariableScaleDecimal;
import com.github.kinolaev.iceberg.flink.sink.dynamic.kafka.connect.io.debezium.time.MicroTime;
import com.github.kinolaev.iceberg.flink.sink.dynamic.kafka.connect.io.debezium.time.MicroTimestamp;
import com.github.kinolaev.iceberg.flink.sink.dynamic.kafka.connect.io.debezium.time.NanoTime;
import com.github.kinolaev.iceberg.flink.sink.dynamic.kafka.connect.io.debezium.time.ZonedTimestamp;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.iceberg.avro.IcebergLogicalTypes;
import org.apache.kafka.common.header.Headers;

public class KafkaAvroDeserializer extends AbstractKafkaAvroDeserializer {
  static {
    // https://github.com/confluentinc/schema-registry/pull/4454
    try {
      Class.forName("io.confluent.kafka.schemaregistry.avro.AvroSchemaUtils");
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
    IcebergLogicalTypes.register();
  }

  private LoadingCache<Schema, Optional<Schema>> schemaCache =
      CacheBuilder.newBuilder().weakKeys().build(new SchemaCacheLoader());

  public KafkaAvroDeserializer(Map<String, ?> props) {
    super();
    configure(deserializerConfig(props), null);
  }

  public GenericRecord deserialize(String topic, boolean isKey, Headers headers, byte[] payload)
      throws ExecutionException {
    GenericContainer value = (GenericContainer) deserialize(topic, isKey, headers, payload, null);
    Schema schema = schemaCache.get(value.getSchema()).orElse(value.getSchema());
    return (GenericRecord) convertValue(value.getSchema(), value, schema);
  }

  private static final String CONNECT_NAME_PROP = "connect.name";
  private static final Map<String, Converter> CONNECT_CONVERTERS =
      Map.of(
          "io.debezium.data.VariableScaleDecimal", VariableScaleDecimal.INSTANCE,
          // Only fixed representation is supported
          // https://github.com/apache/iceberg/blob/apache-iceberg-1.11.0/core/src/main/java/org/apache/iceberg/avro/TypeToSchema.java#L54-L55
          "io.debezium.data.Uuid", Uuid.INSTANCE,
          "io.debezium.time.Date", AddLogicalType.DATE,
          "io.debezium.time.Time", AddLogicalType.TIME_MILLIS,
          // only millis is supported for now
          // https://github.com/apache/iceberg/blob/apache-iceberg-1.11.0/flink/v2.1/flink/src/main/java/org/apache/iceberg/flink/formats/avro/AvroToRowDataConverters.java#L273-L289
          "io.debezium.time.MicroTime", MicroTime.INSTANCE,
          // "io.debezium.time.MicroTime", AddLogicalType.TIME_MICROS,
          "io.debezium.time.NanoTime", NanoTime.INSTANCE,
          "io.debezium.time.Timestamp", AddLogicalType.TIMESTAMP_MILLIS,
          // https://github.com/apache/iceberg/pull/17194
          // "io.debezium.time.MicroTimestamp", AddLogicalType.TIMESTAMP_MICROS,
          "io.debezium.time.MicroTimestamp", MicroTimestamp.INSTANCE,
          "io.debezium.time.NanoTimestamp", AddLogicalType.TIMESTAMP_NANOS,
          "io.debezium.time.ZonedTimestamp", ZonedTimestamp.MICROS);

  private static class SchemaCacheLoader extends CacheLoader<Schema, Optional<Schema>> {
    @Override
    public Optional<Schema> load(Schema schema) throws Exception {
      Schema converted = convertSchema(schema);
      return converted == schema ? Optional.empty() : Optional.of(converted);
    }
  }

  public static Schema convertSchema(Schema schema) {
    String connectName = schema.getProp(CONNECT_NAME_PROP);
    if (connectName != null) {
      Converter converter = CONNECT_CONVERTERS.get(connectName);
      if (converter != null) {
        return converter.convertSchema(schema);
      }
    }
    switch (schema.getType()) {
      case ARRAY:
        return convertArraySchema(schema);
      case MAP:
        return convertMapSchema(schema);
      case RECORD:
        return convertRecordSchema(schema);
      case UNION:
        return convertUnionSchema(schema);
      default:
        return schema;
    }
  }

  private static Schema convertArraySchema(Schema schema) {
    Schema elementSchema = schema.getElementType();
    Schema convertedElementSchema = convertSchema(elementSchema);
    if (convertedElementSchema == elementSchema) {
      return schema;
    }
    Schema converted = Schema.createArray(convertedElementSchema);
    schema.forEachProperty(converted::addProp);
    return converted;
  }

  private static Schema convertMapSchema(Schema schema) {
    Schema valueSchema = schema.getValueType();
    Schema convertedValueSchema = convertSchema(valueSchema);
    if (convertedValueSchema == valueSchema) {
      return schema;
    }
    Schema converted = Schema.createMap(convertedValueSchema);
    schema.forEachProperty(converted::addProp);
    return converted;
  }

  private static Schema convertRecordSchema(Schema schema) {
    List<Schema.Field> fields = new ArrayList<>(schema.getFields().size());
    boolean changed = false;
    for (var field : schema.getFields()) {
      Schema convertedFieldSchema = convertSchema(field.schema());
      changed |= convertedFieldSchema != field.schema();
      fields.add(new Schema.Field(field, convertedFieldSchema));
    }
    if (!changed) {
      return schema;
    }
    Schema converted =
        Schema.createRecord(
            schema.getName(), schema.getDoc(), schema.getNamespace(), schema.isError(), fields);
    schema.forEachProperty(converted::addProp);
    schema.getAliases().forEach(converted::addAlias);
    return converted;
  }

  private static Schema convertUnionSchema(Schema schema) {
    List<Schema> branches = schema.getTypes();
    List<Schema> convertedBranches = null;
    for (int i = 0; i < branches.size(); i++) {
      Schema branch = branches.get(i);
      Schema convertedBranch = convertSchema(branch);
      if (convertedBranch != branch) {
        if (convertedBranches == null) {
          convertedBranches = new ArrayList<>(branches);
        }
        convertedBranches.set(i, convertedBranch);
      }
    }
    return convertedBranches == null ? schema : Schema.createUnion(convertedBranches);
  }

  public static Object convertValue(Schema oldSchema, Object value, Schema newSchema) {
    if (oldSchema == newSchema) {
      return value;
    }
    String connectName = oldSchema.getProp(CONNECT_NAME_PROP);
    if (connectName != null) {
      Converter converter = CONNECT_CONVERTERS.get(connectName);
      if (converter != null) {
        return converter.convertValue(value, newSchema);
      }
    }
    switch (newSchema.getType()) {
      case ARRAY:
        return convertArray(oldSchema, (List<?>) value, newSchema);
      case MAP:
        return convertMap(oldSchema, (Map<?, ?>) value, newSchema);
      case RECORD:
        return convertRecord((GenericRecord) value, newSchema);
      case UNION:
        return convertUnion(oldSchema, value, newSchema);
      default:
        return value;
    }
  }

  private static Object convertArray(Schema oldSchema, List<?> oldArray, Schema newSchema) {
    Schema oldElementSchema = oldSchema.getElementType();
    Schema newElementSchema = newSchema.getElementType();
    GenericData.Array<Object> newArray = new GenericData.Array<>(oldArray.size(), newSchema);
    for (Object element : oldArray) {
      newArray.add(convertValue(oldElementSchema, element, newElementSchema));
    }
    return newArray;
  }

  private static Object convertMap(Schema oldSchema, Map<?, ?> oldMap, Schema newSchema) {
    Schema oldValueSchema = oldSchema.getValueType();
    Schema newValueSchema = newSchema.getValueType();
    Map<Object, Object> newMap = new HashMap<>(oldMap.size());
    for (Map.Entry<?, ?> entry : oldMap.entrySet()) {
      newMap.put(entry.getKey(), convertValue(oldValueSchema, entry.getValue(), newValueSchema));
    }
    return newMap;
  }

  private static GenericRecord convertRecord(GenericRecord oldRecord, Schema newSchema) {
    GenericRecord newRecord = new GenericData.Record(newSchema);
    for (Schema.Field oldField : oldRecord.getSchema().getFields()) {
      Object oldFieldValue = oldRecord.get(oldField.pos());
      Schema newFieldSchema = newSchema.getField(oldField.name()).schema();
      newRecord.put(oldField.pos(), convertValue(oldField.schema(), oldFieldValue, newFieldSchema));
    }
    return newRecord;
  }

  private static Object convertUnion(Schema oldSchema, Object value, Schema newSchema) {
    int branchIndex = GenericData.get().resolveUnion(oldSchema, value);
    return convertValue(
        oldSchema.getTypes().get(branchIndex), value, newSchema.getTypes().get(branchIndex));
  }
}
