package com.github.kinolaev.iceberg.flink.sink.dynamic.kafka.connect.io.debezium.time;

import com.github.kinolaev.iceberg.flink.sink.dynamic.kafka.connect.AddLogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

public class MicroTimestamp extends AddLogicalType {
  public static final MicroTimestamp INSTANCE = new MicroTimestamp();

  private MicroTimestamp() {
    super(schema -> LogicalTypes.timestampMicros());
  }

  @Override
  public Object convertValue(Object value, Schema schema) {
    // https://github.com/apache/iceberg/pull/17194
    return Math.floorDiv((Long) value, 1000L) * 1000;
  }
}
