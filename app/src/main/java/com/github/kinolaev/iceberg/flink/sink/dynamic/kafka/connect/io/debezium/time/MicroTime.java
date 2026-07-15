package com.github.kinolaev.iceberg.flink.sink.dynamic.kafka.connect.io.debezium.time;

import com.github.kinolaev.iceberg.flink.sink.dynamic.kafka.connect.Converter;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

public class MicroTime implements Converter {
  public static final MicroTime INSTANCE = new MicroTime();

  private MicroTime() {}

  @Override
  public Schema convertSchema(Schema schema) {
    Schema converted = Schema.create(Schema.Type.INT);
    LogicalTypes.timeMillis().addToSchema(converted);
    return converted;
  }

  @Override
  public Object convertValue(Object value, Schema schema) {
    return (int) Math.floorDiv((Long) value, 1000L);
  }
}
