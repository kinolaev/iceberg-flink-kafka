package com.github.kinolaev.iceberg.flink.sink.dynamic.kafka;

import java.util.Locale;

enum ForceCase {
  UPPER,
  LOWER;

  static ForceCase fromName(String caseName) {
    return valueOf(caseName.toUpperCase(Locale.ENGLISH));
  }
}
