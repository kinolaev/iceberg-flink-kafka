package com.github.kinolaev.iceberg.flink.sink.dynamic.kafka;

import com.google.common.cache.CacheLoader;
import javax.annotation.Nonnull;
import org.apache.avro.Schema;
import org.apache.iceberg.flink.sink.AvroGenericRecordToRowDataMapper;

class RowDataMapperCacheLoader extends CacheLoader<Schema, AvroGenericRecordToRowDataMapper> {
  @Override
  public @Nonnull AvroGenericRecordToRowDataMapper load(@Nonnull Schema avroSchema) {
    return AvroGenericRecordToRowDataMapper.forAvroSchema(avroSchema);
  }
}
