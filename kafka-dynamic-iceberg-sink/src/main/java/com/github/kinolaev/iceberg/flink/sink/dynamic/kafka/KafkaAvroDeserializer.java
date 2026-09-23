package com.github.kinolaev.iceberg.flink.sink.dynamic.kafka;

import com.github.kinolaev.avro.SchemaConverter;
import com.github.kinolaev.avro.ValueConverter;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer;
import io.confluent.kafka.serializers.schema.id.SchemaId;
import io.confluent.kafka.serializers.schema.id.SchemaIdDeserializer;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nonnull;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;

public class KafkaAvroDeserializer extends AbstractKafkaAvroDeserializer {
  private final LoadingCache<Schema, Optional<Schema>> schemaCache =
      CacheBuilder.newBuilder().weakKeys().build(new SchemaCacheLoader());

  public KafkaAvroDeserializer(Map<String, ?> props) {
    super();
    configure(deserializerConfig(props), null);
  }

  public GenericRecord deserialize(String topic, boolean isKey, Headers headers, byte[] payload)
      throws ExecutionException {
    GenericContainer value = (GenericContainer) deserialize(topic, isKey, headers, payload, null);
    Schema schema = schemaCache.get(value.getSchema()).orElse(value.getSchema());
    return (GenericRecord) ValueConverter.get().apply(value.getSchema(), value, schema);
  }

  // https://github.com/confluentinc/schema-registry/blob/v8.3.2/avro-serializer/src/main/java/io/confluent/kafka/serializers/AbstractKafkaAvroDeserializer.java#L508-L536
  public Schema getSchema(String topic, boolean isKey, Headers headers, byte[] payload) {
    SchemaId schemaId = new SchemaId(AvroSchema.TYPE);
    try (SchemaIdDeserializer schemaIdDeserializer = schemaIdDeserializer(isKey)) {
      schemaIdDeserializer.deserialize(topic, isKey, headers, payload, schemaId);
    } catch (IOException e) {
      throw new SerializationException("Error deserializing Avro message for id " + schemaId, e);
    }
    try {
      String subject =
          strategyUsesSchema(isKey)
              ? getContextName(topic)
              : getSubjectName(topic, isKey, null, null);
      AvroSchema avroSchema = (AvroSchema) getSchemaBySchemaId(subject, schemaId);
      if (subject == null) {
        subject = getSubjectName(topic, isKey, null, avroSchema);
        avroSchema = (AvroSchema) getSchemaBySchemaId(subject, schemaId);
      }
      return avroSchema.rawSchema();
    } catch (IOException | RestClientException e) {
      String schemaType = isKey ? "key" : "value";
      throw new SerializationException(
          "Error retrieving Avro %s schema for id %s".formatted(schemaType, schemaId), e);
    }
  }

  private static class SchemaCacheLoader extends CacheLoader<Schema, Optional<Schema>> {
    @Override
    public @Nonnull Optional<Schema> load(@Nonnull Schema schema) {
      Schema newSchema = SchemaConverter.get().apply(schema);
      return newSchema == schema ? Optional.empty() : Optional.of(newSchema);
    }
  }
}
