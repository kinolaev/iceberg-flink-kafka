package com.github.kinolaev.iceberg.flink.sink.dynamic.kafka;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import javax.annotation.Nonnull;
import org.apache.iceberg.Schema;

class SchemasCacheLoader
    extends CacheLoader<org.apache.avro.Schema, LoadingCache<org.apache.avro.Schema, Schema>> {
  private final boolean forceOptional;
  private final ForceCase forceCase;

  SchemasCacheLoader(boolean forceOptional, ForceCase forceCase) {
    this.forceOptional = forceOptional;
    this.forceCase = forceCase;
  }

  @Override
  public @Nonnull LoadingCache<org.apache.avro.Schema, Schema> load(
      @Nonnull org.apache.avro.Schema valueSchema) {
    return CacheBuilder.newBuilder()
        .weakKeys()
        .build(new SchemaCacheLoader(forceOptional, forceCase, valueSchema));
  }
}
