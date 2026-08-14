package com.github.kinolaev.iceberg.flink.sink.dynamic.kafka;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.SnapshotRef;

record TablesConfig(
    boolean schemaForceOptional,
    ForceCase schemaForceCase,
    String commitBranch,
    List<String> partitionBy,
    DistributionMode distributionMode,
    int writeParallelism,
    boolean upsertModeEnabled) {

  private static final String TABLES_SCHEMA_FORCE_OPTIONAL_PROP =
      "iceberg.tables.schema-force-optional";
  private static final String TABLES_SCHEMA_FORCE_CASE_PROP = "iceberg.tables.schema-force-case";

  private static final String TABLES_DEFAULT_COMMIT_BRANCH_PROP =
      "iceberg.tables.default-commit-branch";
  private static final String TABLES_DEFAULT_PARTITION_BY_PROP =
      "iceberg.tables.default-partition-by";
  private static final String TABLES_DISTRIBUTION_MODE_PROP = "iceberg.tables.distribution-mode";
  private static final String TABLES_WRITE_PARALLELISM_PROP = "iceberg.tables.write-parallelism";
  private static final String TABLES_UPSERT_MODE_ENABLED_PROP =
      "iceberg.tables.upsert-mode-enabled";

  private static final String COMMA_NO_PARENS_REGEX = ",(?![^()]*+\\))";

  TablesConfig(Map<String, String> props) {
    this(
        Optional.ofNullable(props.get(TABLES_SCHEMA_FORCE_OPTIONAL_PROP))
            .map(Boolean::parseBoolean)
            .orElse(false),
        Optional.ofNullable(props.get(TABLES_SCHEMA_FORCE_CASE_PROP))
            .map(ForceCase::fromName)
            .orElse(null),
        props.getOrDefault(TABLES_DEFAULT_COMMIT_BRANCH_PROP, SnapshotRef.MAIN_BRANCH),
        Optional.ofNullable(props.get(TABLES_DEFAULT_PARTITION_BY_PROP))
            .map(TablesConfig::parsePartitionBy)
            .orElse(List.of()),
        Optional.ofNullable(props.get(TABLES_DISTRIBUTION_MODE_PROP))
            .map(DistributionMode::fromName)
            .orElse(null),
        Optional.ofNullable(props.get(TABLES_WRITE_PARALLELISM_PROP))
            .map(Integer::parseInt)
            .orElse(-1),
        Optional.ofNullable(props.get(TABLES_UPSERT_MODE_ENABLED_PROP))
            .map(Boolean::parseBoolean)
            .orElse(false));
  }

  static List<String> parsePartitionBy(String partitionBy) {
    return Arrays.stream(partitionBy.split(COMMA_NO_PARENS_REGEX)).map(String::trim).toList();
  }
}
