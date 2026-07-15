package com.github.kinolaev.iceberg.flink.sink.dynamic.kafka.connect;

import java.util.function.Function;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

public class AddLogicalType implements Converter {
  public AddLogicalType decimal(
      Function<Schema, Integer> precision, Function<Schema, Integer> scale) {
    return new AddLogicalType(
        schema -> LogicalTypes.decimal(precision.apply(schema), scale.apply(schema)));
  }

  public static final AddLogicalType BIG_DECIMAL =
      new AddLogicalType(schema -> LogicalTypes.bigDecimal());
  public static final AddLogicalType UUID = new AddLogicalType(schema -> LogicalTypes.uuid());
  public static final AddLogicalType DATE = new AddLogicalType(schema -> LogicalTypes.date());
  public static final AddLogicalType TIME_MILLIS =
      new AddLogicalType(schema -> LogicalTypes.timeMillis());
  public static final AddLogicalType TIME_MICROS =
      new AddLogicalType(schema -> LogicalTypes.timeMicros());
  public static final AddLogicalType TIMESTAMP_MILLIS =
      new AddLogicalType(schema -> LogicalTypes.timestampMillis());
  public static final AddLogicalType TIMESTAMP_MICROS =
      new AddLogicalType(schema -> LogicalTypes.timestampMicros());
  public static final AddLogicalType TIMESTAMP_NANOS =
      new AddLogicalType(schema -> LogicalTypes.timestampNanos());
  public static final AddLogicalType LOCAL_TIMESTAMP_MILLIS =
      new AddLogicalType(schema -> LogicalTypes.localTimestampMillis());
  public static final AddLogicalType LOCAL_TIMESTAMP_MICROS =
      new AddLogicalType(schema -> LogicalTypes.localTimestampMicros());
  public static final AddLogicalType LOCAL_TIMESTAMP_NANOS =
      new AddLogicalType(schema -> LogicalTypes.localTimestampNanos());
  public static final AddLogicalType DURATION =
      new AddLogicalType(schema -> LogicalTypes.duration());

  private final Function<Schema, LogicalType> logicalType;

  public AddLogicalType(Function<Schema, LogicalType> logicalType) {
    this.logicalType = logicalType;
  }

  @Override
  public Schema convertSchema(Schema schema) {
    Schema converted = new Schema.Parser().parse(schema.toString());
    logicalType.apply(converted).addToSchema(converted);
    return converted;
  }
}
