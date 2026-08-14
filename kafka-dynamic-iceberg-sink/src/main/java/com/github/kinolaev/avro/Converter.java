package com.github.kinolaev.avro;

import org.apache.avro.Schema;

public interface Converter {
  default Schema convertSchema(Schema schema) {
    return schema;
  }
  ;

  default Object convertValue(Object value, Schema schema) {
    return value;
  }
}
