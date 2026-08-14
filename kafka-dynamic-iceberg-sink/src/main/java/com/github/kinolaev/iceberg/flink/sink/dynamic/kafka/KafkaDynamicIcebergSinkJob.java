package com.github.kinolaev.iceberg.flink.sink.dynamic.kafka;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.enumerator.subscriber.KafkaSubscriber;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.ParameterTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.sink.dynamic.DynamicIcebergSink;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaDynamicIcebergSinkJob {
  private static final String NAME_PROP = "name";
  private static final String NAME_DEFAULT = "kafka-dynamic-iceberg-sink";
  private static final String CHECKPOINT_INTERVAL_PROP = "checkpoint.interval";
  private static final int CHECKPOINT_INTERVAL_DEFAULT = 300_000;

  private static final String KAFKA_PREFIX = "kafka.";
  private static final String KAFKA_TOPICS_PROP = "kafka.topics";
  private static final String KAFKA_OFFSETS_PROP = "kafka.offsets";
  private static final String KAFKA_OFFSETS_DEFAULT = "committed,earliest";
  private static final String KAFKA_KEY_DESERIALIZER_PROP =
      KAFKA_PREFIX + ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
  private static final String KAFKA_KEY_DESERIALIZER_DEFAULT =
      "io.confluent.kafka.serializers.KafkaAvroDeserializer";
  private static final String KAFKA_VALUE_DESERIALIZER_PROP =
      KAFKA_PREFIX + ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
  private static final String KAFKA_VALUE_DESERIALIZER_DEFAULT =
      "io.confluent.kafka.serializers.KafkaAvroDeserializer";
  private static final Set<String> KAFKA_IGNORED =
      Set.of(
          KAFKA_TOPICS_PROP,
          KAFKA_OFFSETS_PROP,
          KAFKA_KEY_DESERIALIZER_PROP,
          KAFKA_VALUE_DESERIALIZER_PROP);

  private static final String ICEBERG_CATALOG_PROP = "iceberg.catalog";
  private static final String ICEBERG_CATALOG_DEFAULT = "iceberg";
  private static final String ICEBERG_CATALOG_PREFIX = "iceberg.catalog.";
  private static final String ICEBERG_HADOOP_PREFIX = "iceberg.hadoop.";

  private static final String TABLES_AUTO_CREATE_ENABLED_PROP =
      "iceberg.tables.auto-create-enabled";
  private static final String TABLES_EVOLVE_SCHEMA_ENABLED_PROP =
      "iceberg.tables.evolve-schema-enabled";
  private static final String TABLES_SCHEMA_CASE_INSENSITIVE_PROP =
      "iceberg.tables.schema-case-insensitive";
  private static final boolean TABLES_SCHEMA_CASE_INSENSITIVE_DEFAULT = false;
  private static final String TABLES_WRITE_PROPS_PREFIX = "iceberg.tables.write-props.";

  private static final Logger LOG = LoggerFactory.getLogger(KafkaDynamicIcebergSinkJob.class);

  public static void main(String[] args) throws Exception {
    ParameterTool parameters = ParameterTool.fromPropertiesFile(args[0]);
    if (args.length > 1) {
      parameters = parameters.mergeWith(ParameterTool.fromPropertiesFile(args[1]));
    }

    if (!KAFKA_KEY_DESERIALIZER_DEFAULT.equals(parameters.get(KAFKA_KEY_DESERIALIZER_PROP))) {
      throw new IllegalArgumentException(
          "%s must be %s".formatted(KAFKA_KEY_DESERIALIZER_PROP, KAFKA_KEY_DESERIALIZER_DEFAULT));
    }
    if (!KAFKA_VALUE_DESERIALIZER_DEFAULT.equals(parameters.get(KAFKA_VALUE_DESERIALIZER_PROP))) {
      throw new IllegalArgumentException(
          "%s must be %s"
              .formatted(KAFKA_VALUE_DESERIALIZER_PROP, KAFKA_VALUE_DESERIALIZER_DEFAULT));
    }
    if (!parameters.getBoolean(TABLES_AUTO_CREATE_ENABLED_PROP, false)) {
      LOG.warn("Disabling {} is not supported, ignoring", TABLES_AUTO_CREATE_ENABLED_PROP);
    }
    if (!parameters.getBoolean(TABLES_EVOLVE_SCHEMA_ENABLED_PROP, false)) {
      LOG.warn("Disabling {} is not supported, ignoring", TABLES_EVOLVE_SCHEMA_ENABLED_PROP);
    }

    final String jobName = parameters.get(NAME_PROP, NAME_DEFAULT);
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.enableCheckpointing(
        parameters.getInt(CHECKPOINT_INTERVAL_PROP, CHECKPOINT_INTERVAL_DEFAULT));

    KafkaSource<ConsumerRecord<byte[], byte[]>> source =
        KafkaSource.<ConsumerRecord<byte[], byte[]>>builder()
            .setGroupId("flink-" + jobName)
            .setKafkaSubscriber(parseSubscriber(parameters.get(KAFKA_TOPICS_PROP)))
            .setStartingOffsets(
                parseOffsets(parameters.get(KAFKA_OFFSETS_PROP, KAFKA_OFFSETS_DEFAULT)))
            .setProperties(parseKafkaProperties(parameters.toMap()))
            .setDeserializer(new KafkaConsumerRecordDeserializationSchema())
            .build();
    DataStream<ConsumerRecord<byte[], byte[]>> sourceStream =
        env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka");

    String catalogName = parameters.get(ICEBERG_CATALOG_PROP, ICEBERG_CATALOG_DEFAULT);
    Configuration hadoopConf = new Configuration();
    PropertyUtil.propertiesWithPrefix(parameters.toMap(), ICEBERG_HADOOP_PREFIX)
        .forEach(hadoopConf::set);
    Map<String, String> catalogProps =
        PropertyUtil.propertiesWithPrefix(parameters.toMap(), ICEBERG_CATALOG_PREFIX);

    boolean tablesSchemaCaseInsensitive =
        parameters.getBoolean(
            TABLES_SCHEMA_CASE_INSENSITIVE_PROP, TABLES_SCHEMA_CASE_INSENSITIVE_DEFAULT);
    Map<String, String> tablesWriteProps =
        PropertyUtil.propertiesWithPrefix(parameters.toMap(), TABLES_WRITE_PROPS_PREFIX);

    DynamicIcebergSink.forInput(sourceStream)
        .uidPrefix(jobName)
        .generator(new KafkaDynamicRecordGenerator(parameters.toMap()))
        .catalogLoader(CatalogLoader.rest(catalogName, hadoopConf, catalogProps))
        .caseSensitive(!tablesSchemaCaseInsensitive)
        .tableCreator(new TableCreatorWithNamespaceSortOrderAndProps(parameters.toMap()))
        .setAll(tablesWriteProps)
        .append();
    env.execute(jobName);
  }

  static KafkaSubscriber parseSubscriber(String topics) {
    return switch (topics) {
      case String s when s.contains("*") || s.contains("+") || s.contains("?") ->
          KafkaSubscriber.getTopicPatternSubscriber(Pattern.compile(s));
      case String s when s.contains(":") ->
          KafkaSubscriber.getPartitionSetSubscriber(parsePartitions(s));
      default -> KafkaSubscriber.getTopicListSubscriber(List.of(topics.split(",")));
    };
  }

  private static Set<TopicPartition> parsePartitions(String partitions) {
    return Arrays.stream(partitions.split(","))
        .map(KafkaDynamicIcebergSinkJob::parsePartition)
        .collect(Collectors.toSet());
  }

  private static TopicPartition parsePartition(String partition) {
    String[] p = partition.split(":", 2);
    try {
      return new TopicPartition(p[0], Integer.parseInt(p[1]));
    } catch (Exception cause) {
      throw new IllegalArgumentException("Invalid partition: " + partition, cause);
    }
  }

  static OffsetsInitializer parseOffsets(String offsets) {
    return switch (offsets) {
      case "earliest" -> OffsetsInitializer.earliest();
      case "committed" -> OffsetsInitializer.committedOffsets(OffsetResetStrategy.NONE);
      case "committed,earliest" ->
          OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST);
      case "committed,latest" -> OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST);
      case "latest" -> OffsetsInitializer.latest();
      case String s when s.endsWith(",earliest") ->
          OffsetsInitializer.offsets(
              parsePartitionOffsets(s.substring(0, s.length() - 9)), OffsetResetStrategy.EARLIEST);
      case String s when s.endsWith(",latest") ->
          OffsetsInitializer.offsets(
              parsePartitionOffsets(s.substring(0, s.length() - 7)), OffsetResetStrategy.LATEST);
      case String s when s.contains(":") ->
          OffsetsInitializer.offsets(parsePartitionOffsets(s), OffsetResetStrategy.NONE);
      default -> OffsetsInitializer.timestamp(Long.parseLong(offsets));
    };
  }

  private static Map<TopicPartition, Long> parsePartitionOffsets(String partitionOffsets) {
    return Arrays.stream(partitionOffsets.split(","))
        .map(KafkaDynamicIcebergSinkJob::parsePartitionOffset)
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private static Map.Entry<TopicPartition, Long> parsePartitionOffset(String partitionOffset) {
    String[] p = partitionOffset.split(":", 3);
    try {
      return Map.entry(new TopicPartition(p[0], Integer.parseInt(p[1])), Long.valueOf(p[2]));
    } catch (Exception cause) {
      throw new IllegalArgumentException("Invalid partition offset: " + partitionOffset, cause);
    }
  }

  static Properties parseKafkaProperties(Map<String, String> properties) {
    return properties.entrySet().stream()
        .filter(
            entry ->
                entry.getKey().startsWith(KAFKA_PREFIX) && !KAFKA_IGNORED.contains(entry.getKey()))
        .collect(
            Collectors.toMap(
                entry -> entry.getKey().substring(KAFKA_PREFIX.length()),
                Map.Entry::getValue,
                (prev, next) -> next,
                Properties::new));
  }
}
