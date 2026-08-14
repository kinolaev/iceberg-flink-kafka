package com.github.kinolaev.avro;

import com.github.kinolaev.avro.kafka.connect.io.debezium.data.Uuid;
import com.github.kinolaev.avro.kafka.connect.io.debezium.data.VariableScaleDecimal;
import com.github.kinolaev.avro.kafka.connect.io.debezium.time.MicroTime;
import com.github.kinolaev.avro.kafka.connect.io.debezium.time.MicroTimestamp;
import com.github.kinolaev.avro.kafka.connect.io.debezium.time.NanoTime;
import com.github.kinolaev.avro.kafka.connect.io.debezium.time.ZonedTimestamp;
import java.util.Map;
import org.apache.avro.Schema;

public class Converters {
  private static final String KAFKA_CONNECT_NAME_PROP = "connect.name";
  private static final Map<String, Converter> KAFKA_CONNECT_CONVERTERS =
      Map.of(
          "io.debezium.data.VariableScaleDecimal", VariableScaleDecimal.INSTANCE,
          // Only fixed representation is supported
          // https://github.com/apache/iceberg/blob/apache-iceberg-1.11.0/core/src/main/java/org/apache/iceberg/avro/TypeToSchema.java#L54-L55
          "io.debezium.data.Uuid", Uuid.INSTANCE,
          "io.debezium.time.Date", AddLogicalType.DATE,
          "io.debezium.time.Time", AddLogicalType.TIME_MILLIS,
          // only millis is supported for now
          // https://github.com/apache/iceberg/blob/apache-iceberg-1.11.0/flink/v2.1/flink/src/main/java/org/apache/iceberg/flink/formats/avro/AvroToRowDataConverters.java#L273-L289
          // "io.debezium.time.MicroTime", AddLogicalType.TIME_MICROS,
          "io.debezium.time.MicroTime", MicroTime.INSTANCE,
          "io.debezium.time.NanoTime", NanoTime.INSTANCE,
          "io.debezium.time.Timestamp", AddLogicalType.LOCAL_TIMESTAMP_MILLIS,
          // https://github.com/apache/iceberg/pull/17194
          // "io.debezium.time.MicroTimestamp", AddLogicalType.LOCAL_TIMESTAMP_MICROS,
          "io.debezium.time.MicroTimestamp", MicroTimestamp.INSTANCE,
          "io.debezium.time.NanoTimestamp", AddLogicalType.LOCAL_TIMESTAMP_NANOS,
          "io.debezium.time.ZonedTimestamp", ZonedTimestamp.MICROS);

  public static Converter get(Schema schema) {
    String connectName = schema.getProp(KAFKA_CONNECT_NAME_PROP);
    if (connectName != null) {
      return KAFKA_CONNECT_CONVERTERS.get(connectName);
    }
    return null;
  }
}
