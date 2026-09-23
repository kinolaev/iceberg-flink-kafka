FROM quay.io/debezium/server:3.6.3.Final

ADD --chown=jboss:root \
    https://packages.confluent.io/maven/io/confluent/kafka-connect-avro-converter/8.3.2/kafka-connect-avro-converter-8.3.2.jar \
    https://packages.confluent.io/maven/io/confluent/kafka-connect-avro-data/8.3.2/kafka-connect-avro-data-8.3.2.jar \
    https://packages.confluent.io/maven/io/confluent/kafka-connect-schema-backup-api/8.3.2/kafka-connect-schema-backup-api-8.3.2.jar \
    https://packages.confluent.io/maven/io/confluent/kafka-connect-schema-backup-core/8.3.2/kafka-connect-schema-backup-core-8.3.2.jar \
    https://packages.confluent.io/maven/io/confluent/kafka-avro-serializer/8.3.2/kafka-avro-serializer-8.3.2.jar \
    https://packages.confluent.io/maven/io/confluent/kafka-avro-types/8.3.2/kafka-avro-types-8.3.2.jar \
    https://packages.confluent.io/maven/io/confluent/kafka-schema-converter/8.3.2/kafka-schema-converter-8.3.2.jar \
    https://packages.confluent.io/maven/io/confluent/kafka-schema-registry-client/8.3.2/kafka-schema-registry-client-8.3.2.jar \
    https://packages.confluent.io/maven/io/confluent/kafka-schema-serializer/8.3.2/kafka-schema-serializer-8.3.2.jar \
    https://packages.confluent.io/maven/io/confluent/kafka-schema-types/8.3.2/kafka-schema-types-8.3.2.jar \
    https://packages.confluent.io/maven/io/confluent/common-utils/8.3.2/common-utils-8.3.2.jar \
    https://repo1.maven.org/maven2/org/apache/avro/avro/1.12.2/avro-1.12.2.jar \
    https://repo1.maven.org/maven2/org/apache/commons/commons-compress/1.28.0/commons-compress-1.28.0.jar \
    https://repo1.maven.org/maven2/org/apache/httpcomponents/client5/httpclient5/5.5/httpclient5-5.5.jar \
    https://repo1.maven.org/maven2/org/apache/httpcomponents/core5/httpcore5/5.3.4/httpcore5-5.3.4.jar \
    https://repo1.maven.org/maven2/org/apache/httpcomponents/core5/httpcore5-h2/5.3.4/httpcore5-h2-5.3.4.jar \
    https://repo1.maven.org/maven2/com/fasterxml/jackson/dataformat/jackson-dataformat-csv/2.21.2/jackson-dataformat-csv-2.21.2.jar \
    https://repo1.maven.org/maven2/io/confluent/logredactor/1.0.18/logredactor-1.0.18.jar \
    https://repo1.maven.org/maven2/io/confluent/logredactor-metrics/1.0.18/logredactor-metrics-1.0.18.jar \
    https://repo1.maven.org/maven2/com/eclipsesource/minimal-json/minimal-json/0.9.5/minimal-json-0.9.5.jar \
    /debezium/lib/
