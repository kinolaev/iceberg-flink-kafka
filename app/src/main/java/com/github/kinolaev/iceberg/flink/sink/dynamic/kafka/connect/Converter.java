package com.github.kinolaev.iceberg.flink.sink.dynamic.kafka.connect;

import org.apache.avro.Schema;

public interface Converter {
  Schema convertSchema(Schema schema);

  default Object convertValue(Object value, Schema schema) {
    return value;
  }
}
