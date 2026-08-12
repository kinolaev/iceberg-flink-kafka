package com.github.kinolaev.iceberg.flink.sink.dynamic.kafka.connect;

import java.util.function.ToIntFunction;
import java.util.function.UnaryOperator;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;

public class AddLogicalType implements Converter {
  public AddLogicalType decimal(ToIntFunction<Schema> precision, ToIntFunction<Schema> scale) {
    return new AddLogicalType(
        schema ->
            LogicalTypes.decimal(precision.applyAsInt(schema), scale.applyAsInt(schema))
                .addToSchema(schema));
  }

  public static final AddLogicalType BIG_DECIMAL =
      new AddLogicalType(LogicalTypes.bigDecimal()::addToSchema);
  public static final AddLogicalType UUID = new AddLogicalType(LogicalTypes.uuid()::addToSchema);
  public static final AddLogicalType DATE = new AddLogicalType(LogicalTypes.date()::addToSchema);
  public static final AddLogicalType TIME_MILLIS =
      new AddLogicalType(LogicalTypes.timeMillis()::addToSchema);
  public static final AddLogicalType TIME_MICROS =
      new AddLogicalType(LogicalTypes.timeMicros()::addToSchema);
  public static final AddLogicalType TIMESTAMP_MILLIS =
      new AddLogicalType(
          schema -> {
            schema.addProp(AvroSchemaUtil.ADJUST_TO_UTC_PROP, true);
            return LogicalTypes.timestampMillis().addToSchema(schema);
          });
  public static final AddLogicalType TIMESTAMP_MICROS =
      new AddLogicalType(
          schema -> {
            schema.addProp(AvroSchemaUtil.ADJUST_TO_UTC_PROP, true);
            return LogicalTypes.timestampMicros().addToSchema(schema);
          });
  public static final AddLogicalType TIMESTAMP_NANOS =
      new AddLogicalType(
          schema -> {
            schema.addProp(AvroSchemaUtil.ADJUST_TO_UTC_PROP, true);
            return LogicalTypes.timestampNanos().addToSchema(schema);
          });
  public static final AddLogicalType LOCAL_TIMESTAMP_MILLIS =
      new AddLogicalType(
          schema -> {
            schema.addProp(AvroSchemaUtil.ADJUST_TO_UTC_PROP, false);
            return LogicalTypes.timestampMillis().addToSchema(schema);
          });
  public static final AddLogicalType LOCAL_TIMESTAMP_MICROS =
      new AddLogicalType(
          schema -> {
            schema.addProp(AvroSchemaUtil.ADJUST_TO_UTC_PROP, false);
            return LogicalTypes.timestampMicros().addToSchema(schema);
          });
  public static final AddLogicalType LOCAL_TIMESTAMP_NANOS =
      new AddLogicalType(
          schema -> {
            schema.addProp(AvroSchemaUtil.ADJUST_TO_UTC_PROP, false);
            return LogicalTypes.timestampNanos().addToSchema(schema);
          });
  public static final AddLogicalType DURATION =
      new AddLogicalType(LogicalTypes.duration()::addToSchema);

  private final UnaryOperator<Schema> update;

  public AddLogicalType(UnaryOperator<Schema> update) {
    this.update = update;
  }

  @Override
  public Schema convertSchema(Schema schema) {
    return update.apply(new Schema.Parser().parse(schema.toString()));
  }
}
