package com.github.kinolaev.avro.kafka.connect.io.debezium.data;

import com.github.kinolaev.avro.Converter;
import java.nio.ByteBuffer;
import java.util.UUID;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

public class Uuid implements Converter {
  public static final Uuid INSTANCE = new Uuid();

  private Uuid() {}

  @Override
  public Schema convertSchema(Schema schema) {
    if (schema.getType() == Schema.Type.STRING) {
      return LogicalTypes.uuid().addToSchema(Schema.createFixed("uuid_fixed", null, null, 16));
    }
    return schema;
  }

  @Override
  public Object convertValue(Object value, Schema schema) {
    if (value instanceof CharSequence chars) {
      UUID uuid = UUID.fromString(chars.toString());
      ByteBuffer buffer = ByteBuffer.allocate(16);
      buffer.putLong(uuid.getMostSignificantBits());
      buffer.putLong(uuid.getLeastSignificantBits());
      return new GenericData.Fixed(schema, buffer.array());
    }
    return value;
  }
}
