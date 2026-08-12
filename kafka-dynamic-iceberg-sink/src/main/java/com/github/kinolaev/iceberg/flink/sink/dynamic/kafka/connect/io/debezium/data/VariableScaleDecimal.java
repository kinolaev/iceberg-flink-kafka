package com.github.kinolaev.iceberg.flink.sink.dynamic.kafka.connect.io.debezium.data;

import com.github.kinolaev.iceberg.flink.sink.dynamic.kafka.connect.Converter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;

public class VariableScaleDecimal implements Converter {
  public static final VariableScaleDecimal INSTANCE = new VariableScaleDecimal();

  private VariableScaleDecimal() {}

  @Override
  public Schema convertSchema(Schema schema) {
    Schema converted = Schema.create(Schema.Type.BYTES);
    return LogicalTypes.bigDecimal().addToSchema(converted);
  }

  @Override
  public Object convertValue(Object value, Schema schema) {
    try {
      GenericRecord record = (GenericRecord) value;
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
      encoder.writeBytes((ByteBuffer) record.get("value"));
      encoder.writeInt((int) record.get("scale"));
      encoder.flush();
      return ByteBuffer.wrap(out.toByteArray());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
