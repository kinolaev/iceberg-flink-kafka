package com.github.kinolaev.iceberg.flink.sink.dynamic.kafka;

import com.google.common.cache.CacheLoader;
import java.util.Map;
import javax.annotation.Nonnull;

class TableConfigCacheLoader extends CacheLoader<String, TableConfig> {
  private final Map<String, String> props;
  private final TablesConfig defaultConfig;

  TableConfigCacheLoader(Map<String, String> props) {
    this.props = props;
    this.defaultConfig = new TablesConfig(props);
  }

  @Override
  public @Nonnull TableConfig load(@Nonnull String tableName) {
    return new TableConfig(props, tableName, defaultConfig);
  }
}
