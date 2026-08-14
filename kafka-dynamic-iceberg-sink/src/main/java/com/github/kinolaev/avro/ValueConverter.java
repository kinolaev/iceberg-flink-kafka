package com.github.kinolaev.avro;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

public class ValueConverter {
  private static final ValueConverter DEFAULT = new ValueConverter(Converters::get);

  public static ValueConverter get() {
    return DEFAULT;
  }

  private final Function<Schema, Converter> converterGetter;

  public ValueConverter(Function<Schema, Converter> converterGetter) {
    this.converterGetter = converterGetter;
  }

  public Object apply(Schema schema, Object value, Schema newSchema) {
    if (schema == newSchema) {
      return value;
    }
    Converter converter = converterGetter.apply(schema);
    if (converter != null) {
      return converter.convertValue(value, newSchema);
    }
    return switch (newSchema.getType()) {
      case ARRAY -> array(schema, (List<?>) value, newSchema);
      case MAP -> map(schema, (Map<?, ?>) value, newSchema);
      case RECORD -> record((GenericRecord) value, newSchema);
      case UNION -> union(schema, value, newSchema);
      default -> value;
    };
  }

  private Object array(Schema schema, List<?> array, Schema newSchema) {
    if (schema.getElementType() == newSchema.getElementType()) {
      return array;
    }
    GenericData.Array<Object> newArray = new GenericData.Array<>(array.size(), newSchema);
    for (Object element : array) {
      newArray.add(apply(schema.getElementType(), element, newSchema.getElementType()));
    }
    return newArray;
  }

  private Object map(Schema schema, Map<?, ?> map, Schema newSchema) {
    if (schema.getValueType() == newSchema.getValueType()) {
      return map;
    }
    Map<Object, Object> newMap = new HashMap<>(map.size());
    for (Map.Entry<?, ?> e : map.entrySet()) {
      newMap.put(e.getKey(), apply(schema.getValueType(), e.getValue(), newSchema.getValueType()));
    }
    return newMap;
  }

  private GenericRecord record(GenericRecord record, Schema newSchema) {
    GenericRecord newRecord = new GenericData.Record(newSchema);
    for (Schema.Field field : record.getSchema().getFields()) {
      Object fieldValue = record.get(field.pos());
      Schema newFieldSchema = newSchema.getField(field.name()).schema();
      newRecord.put(field.pos(), apply(field.schema(), fieldValue, newFieldSchema));
    }
    return newRecord;
  }

  private Object union(Schema schema, Object value, Schema newSchema) {
    int branchIndex = GenericData.get().resolveUnion(schema, value);
    return apply(schema.getTypes().get(branchIndex), value, newSchema.getTypes().get(branchIndex));
  }
}
