package com.github.kinolaev.iceberg.flink.sink.dynamic.kafka;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.avro.generic.GenericRecord;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.kafka.common.header.Headers;

class RouteConfig {
  private static final String TABLES_PROP = "iceberg.tables";
  private static final String TABLES_CACHE_TIMEOUT = "iceberg.tables.cache-timeout";
  private static final String TABLES_DYNAMIC_ENABLED_PROP = "iceberg.tables.dynamic-enabled";
  private static final String TABLES_ROUTE_FIELD = "iceberg.tables.route-field";
  private static final String TABLES_ROUTE_FIELD_REGEX_PROP = "iceberg.tables.route-field-regex";
  private static final String TABLES_ROUTE_FIELD_REPLACE_PROP =
      "iceberg.tables.route-field-replace";
  private static final String TABLES_ROUTE_FIELD_FORCE_CASE_PROP =
      "iceberg.tables.route-field-force-case";

  private final LoadingCache<String, TableConfig> tableConfigCache;
  private final List<TableConfig> tableConfigs;
  private final String field;
  private final String regex;
  private final String replace;
  private final ForceCase forceCase;

  RouteConfig(Map<String, String> props) {
    this(
        CacheBuilder.newBuilder()
            .expireAfterAccess(
                Long.parseLong(props.getOrDefault(TABLES_CACHE_TIMEOUT, "3600")), TimeUnit.MINUTES)
            .build(new TableConfigCacheLoader(props)),
        Boolean.parseBoolean(props.getOrDefault(TABLES_DYNAMIC_ENABLED_PROP, "false"))
            ? null
            : List.of(props.get(TABLES_PROP).split(",")),
        props.getOrDefault(TABLES_ROUTE_FIELD, "topic"),
        props.get(TABLES_ROUTE_FIELD_REGEX_PROP),
        props.getOrDefault(TABLES_ROUTE_FIELD_REPLACE_PROP, ""),
        Optional.ofNullable(props.get(TABLES_ROUTE_FIELD_FORCE_CASE_PROP))
            .map(ForceCase::fromName)
            .orElse(null));
  }

  RouteConfig(
      LoadingCache<String, TableConfig> tableConfigCache,
      List<String> tables,
      String field,
      String regex,
      String replace,
      ForceCase forceCase) {
    this.tableConfigCache = tableConfigCache;
    this.tableConfigs =
        tables == null ? null : tables.stream().map(tableConfigCache::getUnchecked).toList();
    this.field = field;
    this.regex = regex;
    this.replace = replace;
    this.forceCase = forceCase;
  }

  boolean needsKey() {
    return field.startsWith("key.");
  }

  List<TableConfig> getTableConfigs(
      String topic, Headers headers, GenericRecord key, GenericRecord value) {
    final String routeValue = getRouteValue(topic, headers, key, value);
    return tableConfigs == null
        ? List.of(tableConfigCache.getUnchecked(routeValue))
        : tableConfigs.stream()
            .filter(t -> routeValue.matches(t.routeRegex()))
            .map(TableConfig::identifier)
            .map(TableIdentifier::toString)
            .map(tableConfigCache::getUnchecked)
            .toList();
  }

  public String getRouteValue(
      String topic, Headers headers, GenericRecord key, GenericRecord value) {
    String[] path = field.split("\\.", 2);
    String routeValue;
    try {
      routeValue =
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
      routeValue = routeValue.replaceAll(regex, replace);
    }
    return switch (forceCase) {
      case null -> routeValue;
      case UPPER -> routeValue.toUpperCase();
      case LOWER -> routeValue.toLowerCase();
    };
  }
}
