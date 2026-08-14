package com.github.kinolaev.avro;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;

public class SchemaConverter implements Function<Schema, Schema> {
  private static final SchemaConverter DEFAULT = new SchemaConverter(Converters::get);

  public static SchemaConverter get() {
    return DEFAULT;
  }

  private final Function<Schema, Converter> converterGetter;
  private final ValueConverter valueConverter;

  public SchemaConverter(Function<Schema, Converter> converterGetter) {
    this.converterGetter = converterGetter;
    this.valueConverter = new ValueConverter(converterGetter);
  }

  @Override
  public Schema apply(Schema schema) {
    Converter converter = converterGetter.apply(schema);
    if (converter != null) {
      return converter.convertSchema(schema);
    }
    return switch (schema.getType()) {
      case ARRAY -> array(schema);
      case MAP -> map(schema);
      case RECORD -> record(schema);
      case UNION -> union(schema);
      default -> schema;
    };
  }

  private Schema array(Schema schema) {
    Schema newElementSchema = apply(schema.getElementType());
    if (newElementSchema == schema.getElementType()) {
      return schema;
    }
    Schema newSchema = Schema.createArray(newElementSchema);
    schema.forEachProperty(newSchema::addProp);
    return newSchema;
  }

  private Schema map(Schema schema) {
    Schema newValueSchema = apply(schema.getValueType());
    if (newValueSchema == schema.getValueType()) {
      return schema;
    }
    Schema newSchema = Schema.createMap(newValueSchema);
    schema.forEachProperty(newSchema::addProp);
    return newSchema;
  }

  private Schema record(Schema schema) {
    List<Schema.Field> fields = new ArrayList<>(schema.getFields().size());
    boolean changed = false;
    for (var field : schema.getFields()) {
      Schema newFieldSchema = apply(field.schema());
      changed |= newFieldSchema != field.schema();
      fields.add(field(field, newFieldSchema));
    }
    if (!changed) {
      return schema;
    }
    Schema newSchema =
        Schema.createRecord(
            schema.getName(), schema.getDoc(), schema.getNamespace(), schema.isError(), fields);
    schema.forEachProperty(newSchema::addProp);
    schema.getAliases().forEach(newSchema::addAlias);
    return newSchema;
  }

  private Schema.Field field(Schema.Field field, Schema newSchema) {
    if (newSchema == field.schema() || !field.hasDefaultValue()) {
      return new Schema.Field(field, newSchema);
    }
    Object newGenericDefaultValue =
        valueConverter.apply(field.schema(), GenericData.get().getDefaultValue(field), newSchema);
    Object newDefaultValue = unwrap(newGenericDefaultValue, newSchema);
    Schema.Field newField =
        new Schema.Field(field.name(), newSchema, field.doc(), newDefaultValue, field.order());
    newField.putAll(field);
    field.aliases().forEach(newField::addAlias);
    return newField;
  }

  private Schema union(Schema schema) {
    List<Schema> branches = schema.getTypes();
    List<Schema> newBranches = null;
    for (int i = 0; i < branches.size(); i++) {
      Schema branch = branches.get(i);
      Schema newBranch = apply(branch);
      if (newBranch != branch) {
        if (newBranches == null) {
          newBranches = new ArrayList<>(branches);
        }
        newBranches.set(i, newBranch);
      }
    }
    return newBranches == null ? schema : Schema.createUnion(newBranches);
  }

  private static Object unwrap(Object value, Schema schema) {
    if (value == null) {
      return JsonProperties.NULL_VALUE;
    }
    return switch (schema.getType()) {
      case BYTES -> unwrapBytes((ByteBuffer) value);
      case FIXED -> new String(((GenericFixed) value).bytes(), StandardCharsets.ISO_8859_1);
      case ARRAY -> unwrapArray((List<?>) value, schema.getElementType());
      case MAP -> unwrapMap((Map<?, ?>) value, schema.getValueType());
      case RECORD -> unwrapRecord((GenericRecord) value, schema);
      case UNION -> unwrapUnion(value, schema);
      default -> value;
    };
  }

  private static Object unwrapBytes(ByteBuffer buffer) {
    if (buffer.hasArray()) {
      int offset = buffer.arrayOffset() + buffer.position();
      return new String(buffer.array(), offset, buffer.remaining(), StandardCharsets.ISO_8859_1);
    }
    byte[] bytes = new byte[buffer.remaining()];
    buffer.duplicate().get(bytes);
    return new String(bytes, StandardCharsets.ISO_8859_1);
  }

  private static Object unwrapArray(List<?> list, Schema elementSchema) {
    List<Object> newList = new ArrayList<>();
    for (Object element : list) {
      newList.add(unwrap(element, elementSchema));
    }
    return newList;
  }

  private static Object unwrapMap(Map<?, ?> map, Schema valueSchema) {
    Map<Object, Object> newMap = new LinkedHashMap<>();
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      newMap.put(entry.getKey(), unwrap(entry.getValue(), valueSchema));
    }
    return newMap;
  }

  private static Object unwrapRecord(GenericRecord record, Schema schema) {
    Map<String, Object> newMap = new LinkedHashMap<>();
    for (Schema.Field field : schema.getFields()) {
      newMap.put(field.name(), unwrap(record.get(field.pos()), field.schema()));
    }
    return newMap;
  }

  private static Object unwrapUnion(Object value, Schema schema) {
    int branchIndex = GenericData.get().resolveUnion(schema, value);
    return unwrap(value, schema.getTypes().get(branchIndex));
  }
}
