package com.github.kinolaev.iceberg.flink.sink.dynamic.kafka;

import com.google.common.cache.CacheLoader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.types.Types;

class SchemaCacheLoader extends CacheLoader<org.apache.avro.Schema, Schema> {
  private final boolean forceOptional;
  private final ForceCase forceCase;
  private final org.apache.avro.Schema valueSchema;

  SchemaCacheLoader(
      boolean forceOptional, ForceCase forceCase, org.apache.avro.Schema valueSchema) {
    this.forceOptional = forceOptional;
    this.forceCase = forceCase;
    this.valueSchema = valueSchema;
  }

  @Override
  public @Nonnull Schema load(@Nonnull org.apache.avro.Schema keySchema) {
    Map<String, Integer> idToName = new HashMap<>(keySchema.getFields().size());
    List<Types.NestedField> fields = new ArrayList<>(valueSchema.getFields().size());
    Types.StructType struct = AvroSchemaUtil.convert(valueSchema).asNestedType().asStructType();
    for (Types.NestedField field : struct.fields()) {
      boolean isKey = keySchema.getField(field.name()) != null;
      if (isKey) idToName.put(field.name(), field.fieldId());
      fields.add(convertField(isKey, forceOptional, forceCase, field));
    }
    Set<Integer> identifierFieldIds = new LinkedHashSet<>(idToName.size());
    for (org.apache.avro.Schema.Field field : keySchema.getFields()) {
      Integer id = idToName.get(field.name());
      if (id != null) identifierFieldIds.add(id);
    }
    return new Schema(fields, identifierFieldIds);
  }

  private static Types.NestedField convertField(
      boolean isRequired, boolean forceOptional, ForceCase forceCase, Types.NestedField field) {
    Types.NestedField newField = !isRequired && forceOptional ? field.asOptional() : field;
    return switch (forceCase) {
      case null -> newField;
      case UPPER ->
          Types.NestedField.from(newField).withName(newField.name().toUpperCase()).build();
      case LOWER ->
          Types.NestedField.from(newField).withName(newField.name().toLowerCase()).build();
    };
  }
}
