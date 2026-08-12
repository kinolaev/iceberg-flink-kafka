package com.github.kinolaev.iceberg.flink.sink.dynamic.kafka.connect.io.debezium.time;

import com.github.kinolaev.iceberg.flink.sink.dynamic.kafka.connect.Converter;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;

public class MicroTimestamp implements Converter {
  public static final MicroTimestamp INSTANCE = new MicroTimestamp();

  private MicroTimestamp() {}

  @Override
  public Schema convertSchema(Schema schema) {
    Schema converted = Schema.create(Schema.Type.LONG);
    converted.addProp(AvroSchemaUtil.ADJUST_TO_UTC_PROP, false);
    return LogicalTypes.timestampMicros().addToSchema(converted);
  }

  @Override
  public Object convertValue(Object value, Schema schema) {
    // https://github.com/apache/iceberg/pull/17194
    return Math.floorDiv((Long) value, 1000L) * 1000;
  }
}
