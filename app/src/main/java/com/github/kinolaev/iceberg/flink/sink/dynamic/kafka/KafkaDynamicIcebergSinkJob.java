package com.github.kinolaev.iceberg.flink.sink.dynamic.kafka;

import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.ParameterTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.sink.dynamic.DynamicIcebergSink;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class KafkaDynamicIcebergSinkJob {
    private static final String NAME_PROP = "name";
    private static final String NAME_DEFAULT = KafkaDynamicIcebergSinkJob.class.getCanonicalName();
    private static final String CHECKPOINT_INTERVAL_PROP = "checkpoint.interval";
    private static final int CHECKPOINT_INTERVAL_DEFAULT = 300_000;

    private static final String KAFKA_PROP_PREFIX = "kafka.";
    private static final String HADOOP_PROP_PREFIX = "hadoop.";
    private static final String CATALOG_PROP_PREFIX = "iceberg.catalog.";
    private static final String CATALOG_NAME_PROP = "iceberg.catalog";
    private static final String CATALOG_NAME_DEFAULT = "iceberg";

    private static final String TOPICS_PROP = "kafka.topics";
    private static final String TOPIC_PATTERN_PROP = "kafka.topic-pattern";
    private static final String PARTITIONS_PROP = "kafka.partitions";
    private static final Set<String> SUBSCRIBER_PROPS = Set.of(TOPICS_PROP, TOPIC_PATTERN_PROP, PARTITIONS_PROP);


    public static void main(String[] args) throws Exception {
        ParameterTool parameters = ParameterTool.fromPropertiesFile(args[0]);
        if (args.length > 1) {
            parameters = parameters.mergeWith(ParameterTool.fromPropertiesFile(args[1]));
        }

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(parameters.getInt(CHECKPOINT_INTERVAL_PROP, CHECKPOINT_INTERVAL_DEFAULT));

        final Properties kafkaProps = parameters.toMap().entrySet().stream()
            .filter(entry -> entry.getKey().startsWith(KAFKA_PROP_PREFIX) && !SUBSCRIBER_PROPS.contains(entry.getKey()))
            .collect(Collectors.toMap(
                entry -> entry.getKey().substring(KAFKA_PROP_PREFIX.length()),
                Map.Entry::getValue, (prev, next) -> next, Properties::new));
        KafkaSource<ConsumerRecord<byte[], byte[]>> source = KafkaSource.<ConsumerRecord<byte[], byte[]>>builder()
            .setTopicPattern(Pattern.compile(parameters.get(TOPIC_PATTERN_PROP)))
            .setGroupId(parameters.get(NAME_PROP, NAME_DEFAULT))
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setDeserializer(new KafkaConsumerRecordDeserializationSchema())
            .setProperties(kafkaProps)
            .build();
        DataStream<ConsumerRecord<byte[], byte[]>> sourceStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka");

        String catalogName = parameters.get(CATALOG_NAME_PROP, CATALOG_NAME_DEFAULT);
        Configuration hadoopConf = new Configuration();
        PropertyUtil.propertiesWithPrefix(parameters.toMap(), HADOOP_PROP_PREFIX).forEach(hadoopConf::set);
        Map<String, String> catalogProps = PropertyUtil.propertiesWithPrefix(parameters.toMap(), CATALOG_PROP_PREFIX);
        DynamicIcebergSink.forInput(sourceStream)
            .generator(new KafkaDynamicRecordGenerator(parameters.toMap()))
            .catalogLoader(CatalogLoader.rest(catalogName, hadoopConf, catalogProps))
            .append();
        env.execute(parameters.get(NAME_PROP, NAME_DEFAULT));
    }
}
