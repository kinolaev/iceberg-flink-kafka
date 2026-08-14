package com.github.kinolaev.iceberg.flink.sink.dynamic.kafka;

import com.google.common.base.Splitter;
import com.google.common.cache.CacheLoader;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.util.Pair;

class SpecCacheLoader extends CacheLoader<Schema, PartitionSpec> {
  private static final Pattern TRANSFORM_REGEX = Pattern.compile("(\\w+)\\((.+)\\)");

  private final List<String> partitionBy;

  SpecCacheLoader(List<String> partitionBy) {
    this.partitionBy = partitionBy;
  }

  // https://github.com/apache/iceberg/blob/apache-iceberg-1.11.0/kafka-connect/kafka-connect/src/main/java/org/apache/iceberg/connect/data/SchemaUtils.java#L153-L210
  @Override
  public @Nonnull PartitionSpec load(@Nonnull Schema schema) {
    if (partitionBy.isEmpty()) {
      return PartitionSpec.unpartitioned();
    }

    PartitionSpec.Builder specBuilder = PartitionSpec.builderFor(schema);
    partitionBy.forEach(
        partitionField -> {
          Matcher matcher = TRANSFORM_REGEX.matcher(partitionField);
          if (matcher.matches()) {
            String transform = matcher.group(1);
            switch (transform) {
              case "year":
              case "years":
                specBuilder.year(matcher.group(2));
                break;
              case "month":
              case "months":
                specBuilder.month(matcher.group(2));
                break;
              case "day":
              case "days":
                specBuilder.day(matcher.group(2));
                break;
              case "hour":
              case "hours":
                specBuilder.hour(matcher.group(2));
                break;
              case "bucket":
                {
                  Pair<String, Integer> args = transformArgPair(matcher.group(2));
                  specBuilder.bucket(args.first(), args.second());
                  break;
                }
              case "truncate":
                {
                  Pair<String, Integer> args = transformArgPair(matcher.group(2));
                  specBuilder.truncate(args.first(), args.second());
                  break;
                }
              default:
                throw new UnsupportedOperationException("Unsupported transform: " + transform);
            }
          } else {
            specBuilder.identity(partitionField);
          }
        });
    return specBuilder.build();
  }

  private static Pair<String, Integer> transformArgPair(String argsStr) {
    List<String> parts = Splitter.on(',').splitToList(argsStr);
    if (parts.size() != 2) {
      throw new IllegalArgumentException("Invalid argument " + argsStr + ", should have 2 parts");
    }
    return Pair.of(parts.get(0).trim(), Integer.parseInt(parts.get(1).trim()));
  }
}
