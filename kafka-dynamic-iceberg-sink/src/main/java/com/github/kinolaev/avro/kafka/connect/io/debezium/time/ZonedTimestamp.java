package com.github.kinolaev.avro.kafka.connect.io.debezium.time;

import com.github.kinolaev.avro.Converter;
import java.time.Instant;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;

public class ZonedTimestamp implements Converter {
  public static final ZonedTimestamp MILLIS = new ZonedTimestamp(LogicalTypes.timestampMillis());
  public static final ZonedTimestamp MICROS = new ZonedTimestamp(LogicalTypes.timestampMicros());
  public static final ZonedTimestamp NANOS = new ZonedTimestamp(LogicalTypes.timestampNanos());

  private final LogicalType logicalType;

  private ZonedTimestamp(LogicalType logicalType) {
    this.logicalType = logicalType;
  }

  @Override
  public Schema convertSchema(Schema schema) {
    Schema converted = Schema.create(Schema.Type.LONG);
    converted.addProp(AvroSchemaUtil.ADJUST_TO_UTC_PROP, true);
    return logicalType.addToSchema(converted);
  }

  @Override
  public Object convertValue(Object value, Schema schema) {
    Instant instant = Instant.parse((CharSequence) value);
    if (schema.getLogicalType() instanceof LogicalTypes.TimestampMillis) {
      return instant.toEpochMilli();
    }
    if (schema.getLogicalType() instanceof LogicalTypes.TimestampMicros) {
      // https://github.com/apache/iceberg/pull/17194
      // int microsOfSecond = instant.getNano() / 1000;
      int microsOfSecond = instant.getNano() / 1_000_000 * 1000;
      return Math.addExact(Math.multiplyExact(instant.getEpochSecond(), 1000_000), microsOfSecond);
    }
    if (schema.getLogicalType() instanceof LogicalTypes.TimestampNanos) {
      return Math.addExact(
          Math.multiplyExact(instant.getEpochSecond(), 1000_000_000), instant.getNano());
    }
    return value;
  }
}
