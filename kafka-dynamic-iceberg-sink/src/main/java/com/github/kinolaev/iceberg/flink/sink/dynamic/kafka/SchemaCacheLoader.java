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

class SchemaCacheLoader extends CacheLoader<Set<String>, Schema> {
  private final boolean forceOptional;
  private final ForceCase forceCase;
  private final org.apache.avro.Schema avroSchema;

  SchemaCacheLoader(boolean forceOptional, ForceCase forceCase, org.apache.avro.Schema avroSchema) {
    this.forceOptional = forceOptional;
    this.forceCase = forceCase;
    this.avroSchema = avroSchema;
  }

  @Override
  public @Nonnull Schema load(@Nonnull Set<String> idColumns) {
    Map<String, Integer> nameToId = new HashMap<>(idColumns.size());
    List<Types.NestedField> fields = new ArrayList<>(avroSchema.getFields().size());
    Types.StructType struct = AvroSchemaUtil.convert(avroSchema).asNestedType().asStructType();
    for (Types.NestedField field : struct.fields()) {
      boolean isIdColumn = idColumns.contains(field.name());
      if (isIdColumn) nameToId.put(field.name(), field.fieldId());
      fields.add(convertField(isIdColumn, forceOptional, forceCase, field));
    }
    Set<Integer> identifierFieldIds = new LinkedHashSet<>(nameToId.size());
    for (String idColumn : idColumns) {
      Integer id = nameToId.get(idColumn);
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
