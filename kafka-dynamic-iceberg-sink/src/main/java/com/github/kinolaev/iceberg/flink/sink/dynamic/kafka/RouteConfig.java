package com.github.kinolaev.iceberg.flink.sink.dynamic.kafka;

import java.util.Map;
import java.util.Optional;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.header.Headers;

class RouteConfig {
  private static final String TABLES_ROUTE_FIELD = "iceberg.tables.route-field";
  private static final String TABLES_ROUTE_REGEX_PROP = "iceberg.tables.route-regex";
  private static final String TABLES_ROUTE_REGEX_REPLACE_PROP =
      "iceberg.tables.route-regex-replace";
  private static final String TABLES_ROUTE_FORCE_CASE_PROP = "iceberg.tables.route-force-case";

  private final String field;
  private final String regex;
  private final String regexReplace;
  private final ForceCase forceCase;

  RouteConfig(Map<String, String> props) {
    this(
        props.getOrDefault(TABLES_ROUTE_FIELD, "topic"),
        props.get(TABLES_ROUTE_REGEX_PROP),
        props.getOrDefault(TABLES_ROUTE_REGEX_REPLACE_PROP, ""),
        Optional.ofNullable(props.get(TABLES_ROUTE_FORCE_CASE_PROP))
            .map(ForceCase::fromName)
            .orElse(null));
  }

  RouteConfig(String field, String regex, String regexReplace, ForceCase forceCase) {
    this.field = field;
    this.regex = regex;
    this.regexReplace = regexReplace;
    this.forceCase = forceCase;
  }

  boolean needsKey() {
    return field.startsWith("key.");
  }

  String getTableName(String topic, Headers headers, GenericRecord key, GenericRecord value) {
    String[] path = field.split("\\.", 2);
    String tableName;
    try {
      tableName =
          switch (path[0]) {
            case "topic" -> topic;
            case "headers" -> new String(headers.lastHeader(path[1]).value());
            case "key" -> (String) key.get(path[1]);
            case "value" -> (String) value.get(path[1]);
            default -> throw new IllegalArgumentException("Unexpected record key");
          };
    } catch (Exception cause) {
      throw new IllegalArgumentException(TABLES_ROUTE_FIELD + "=" + field, cause);
    }
    if (regex != null) {
      tableName = tableName.replaceAll(regex, regexReplace);
    }
    return switch (forceCase) {
      case null -> tableName;
      case UPPER -> tableName.toUpperCase();
      case LOWER -> tableName.toLowerCase();
    };
  }
}
