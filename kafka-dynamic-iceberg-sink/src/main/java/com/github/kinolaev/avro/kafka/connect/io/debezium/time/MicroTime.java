package com.github.kinolaev.avro.kafka.connect.io.debezium.time;

import com.github.kinolaev.avro.Converter;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

public class MicroTime implements Converter {
  public static final MicroTime INSTANCE = new MicroTime();

  private MicroTime() {}

  @Override
  public Schema convertSchema(Schema schema) {
    // only millis is supported for now
    // https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/flink/v2.2/flink/src/main/java/org/apache/iceberg/flink/formats/avro/AvroToRowDataConverters.java#L273-L289
    return LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT));
  }

  @Override
  public Object convertValue(Object value, Schema schema) {
    return (int) Math.floorDiv((Long) value, 1000L);
  }
}
