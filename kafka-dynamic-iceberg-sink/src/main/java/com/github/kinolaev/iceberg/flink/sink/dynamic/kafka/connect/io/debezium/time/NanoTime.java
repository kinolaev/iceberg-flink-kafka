package com.github.kinolaev.iceberg.flink.sink.dynamic.kafka.connect.io.debezium.time;

import com.github.kinolaev.iceberg.flink.sink.dynamic.kafka.connect.Converter;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

public class NanoTime implements Converter {
  public static final NanoTime INSTANCE = new NanoTime();

  private NanoTime() {}

  @Override
  public Schema convertSchema(Schema schema) {
    // only millis is supported for now
    // https://github.com/apache/iceberg/blob/apache-iceberg-1.11.0/flink/v2.1/flink/src/main/java/org/apache/iceberg/flink/formats/avro/AvroToRowDataConverters.java#L273-L289
    return LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT));
  }

  @Override
  public Object convertValue(Object value, Schema schema) {
    return (int) Math.floorDiv((Long) value, 1_000_000L);
  }
}
