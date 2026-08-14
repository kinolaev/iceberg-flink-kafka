package com.github.kinolaev.iceberg.flink.sink.dynamic.kafka;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;

record TableConfig(
    LoadingCache<org.apache.avro.Schema, LoadingCache<org.apache.avro.Schema, Schema>> schemaCache,
    String commitBranch,
    LoadingCache<Schema, PartitionSpec> specCache,
    DistributionMode distributionMode,
    int writeParallelism,
    boolean upsertModeEnabled) {

  private static final String TABLE_SCHEMA_FORCE_OPTIONAL_PROP =
      "iceberg.table.%s.schema-force-optional";
  private static final String TABLE_SCHEMA_FORCE_CASE_PROP = "iceberg.table.%s.schema-force-case";

  private static final String TABLE_COMMIT_BRANCH_PROP = "iceberg.table.%s.commit-branch";
  private static final String TABLE_PARTITION_BY_PROP = "iceberg.table.%s.partition-by";
  private static final String TABLE_DISTRIBUTION_MODE_PROP = "iceberg.table.%s.distribution-mode";
  private static final String TABLE_WRITE_PARALLELISM_PROP = "iceberg.table.%s.write-parallelism";
  private static final String TABLE_UPSERT_MODE_ENABLED_PROP =
      "iceberg.table.%s.upsert-mode-enabled";

  TableConfig(Map<String, String> props, String tableName, TablesConfig defaultConfig) {
    this(
        CacheBuilder.newBuilder()
            .weakKeys()
            .build(schemasCacheLoader(props, tableName, defaultConfig)),
        props.getOrDefault(
            TABLE_COMMIT_BRANCH_PROP.formatted(tableName), defaultConfig.commitBranch()),
        CacheBuilder.newBuilder()
            .weakKeys()
            .build(specCacheLoader(props, tableName, defaultConfig)),
        Optional.ofNullable(props.get(TABLE_DISTRIBUTION_MODE_PROP.formatted(tableName)))
            .map(DistributionMode::fromName)
            .orElse(defaultConfig.distributionMode()),
        Optional.ofNullable(props.get(TABLE_WRITE_PARALLELISM_PROP.formatted(tableName)))
            .map(Integer::parseInt)
            .orElse(defaultConfig.writeParallelism()),
        Optional.ofNullable(props.get(TABLE_UPSERT_MODE_ENABLED_PROP.formatted(tableName)))
            .map(Boolean::parseBoolean)
            .orElse(defaultConfig.upsertModeEnabled()));
  }

  static SchemasCacheLoader schemasCacheLoader(
      Map<String, String> props, String tableName, TablesConfig defaultConfig) {
    return new SchemasCacheLoader(
        Optional.ofNullable(props.get(TABLE_SCHEMA_FORCE_OPTIONAL_PROP.formatted(tableName)))
            .map(Boolean::parseBoolean)
            .orElse(defaultConfig.schemaForceOptional()),
        Optional.ofNullable(props.get(TABLE_SCHEMA_FORCE_CASE_PROP.formatted(tableName)))
            .map(ForceCase::fromName)
            .orElse(defaultConfig.schemaForceCase()));
  }

  static SpecCacheLoader specCacheLoader(
      Map<String, String> props, String tableName, TablesConfig defaultConfig) {
    return new SpecCacheLoader(
        Optional.ofNullable(props.get(TABLE_PARTITION_BY_PROP.formatted(tableName)))
            .map(TablesConfig::parsePartitionBy)
            .orElse(defaultConfig.partitionBy()));
  }
}
