FROM quay.io/debezium/connect:3.5

ADD --chown=kafka:kafka \
    https://packages.confluent.io/maven/io/confluent/kafka-connect-avro-converter/7.9.8/kafka-connect-avro-converter-7.9.8.jar \
    https://packages.confluent.io/maven/io/confluent/kafka-connect-avro-data/7.9.8/kafka-connect-avro-data-7.9.8.jar \
    https://packages.confluent.io/maven/io/confluent/kafka-avro-serializer/7.9.8/kafka-avro-serializer-7.9.8.jar \
    https://packages.confluent.io/maven/io/confluent/kafka-schema-serializer/7.9.8/kafka-schema-serializer-7.9.8.jar \
    https://packages.confluent.io/maven/io/confluent/kafka-schema-converter/7.9.8/kafka-schema-converter-7.9.8.jar \
    https://packages.confluent.io/maven/io/confluent/kafka-schema-registry-client/7.9.8/kafka-schema-registry-client-7.9.8.jar \
    https://packages.confluent.io/maven/io/confluent/common-config/7.9.8/common-config-7.9.8.jar \
    https://packages.confluent.io/maven/io/confluent/common-utils/7.9.8/common-utils-7.9.8.jar \
    https://repo1.maven.org/maven2/org/apache/avro/avro/1.12.1/avro-1.12.1.jar \
    https://repo1.maven.org/maven2/org/apache/commons/commons-compress/1.28.0/commons-compress-1.28.0.jar \
    https://repo1.maven.org/maven2/com/google/guava/failureaccess/1.0.3/failureaccess-1.0.3.jar \
    https://repo1.maven.org/maven2/com/google/guava/guava/33.6.0-jre/guava-33.6.0-jre.jar \
    https://repo1.maven.org/maven2/com/eclipsesource/minimal-json/minimal-json/0.9.5/minimal-json-0.9.5.jar \
    https://repo1.maven.org/maven2/io/confluent/logredactor/1.0.18/logredactor-1.0.18.jar \
    https://repo1.maven.org/maven2/io/confluent/logredactor-metrics/1.0.18/logredactor-metrics-1.0.18.jar \
    /kafka/libs/
