package com.github.kinolaev.iceberg.flink.sink.dynamic.kafka;

import java.io.IOException;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class KafkaConsumerRecordDeserializationSchema implements KafkaRecordDeserializationSchema<ConsumerRecord<byte[], byte[]>> {
    @Override
    public TypeInformation<ConsumerRecord<byte[], byte[]>> getProducedType() {
        return new TypeHint<ConsumerRecord<byte[], byte[]>>(){}.getTypeInfo();
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<ConsumerRecord<byte[], byte[]>> out) throws IOException {
        out.collect(record);
    }
}
