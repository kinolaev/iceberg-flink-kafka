package com.github.kinolaev.iceberg.flink.sink.dynamic.kafka;

import java.util.Map;
import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.header.Headers;

public class KafkaAvroDeserializer extends AbstractKafkaAvroDeserializer {
    public KafkaAvroDeserializer(Map<String, ?> props) {
        super();
        configure(deserializerConfig(props), null);
    }
    public GenericRecord deserialize(String topic, boolean isKey, Headers headers, byte[] payload) {
        return (GenericRecord) deserialize(topic, isKey, headers, payload, null);
    }
}
