FROM gradle:9.2.1-jdk21-alpine AS build

USER gradle
WORKDIR /home/gradle/project
COPY --chown=gradle:gradle . .
RUN --mount=type=cache,uid=1000,gid=1000,target=/home/gradle/.gradle/caches gradle --no-daemon shadowJar

FROM flink:2.1.3-java21

ARG ICEBERG_VERSION=1.11.0
ARG HADOOP_VERSION=3.4.3

RUN mkdir -p /opt/flink/lib/iceberg && cd /opt/flink/lib/iceberg && \
    curl -LO https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-2.1/${ICEBERG_VERSION}/iceberg-flink-runtime-2.1-${ICEBERG_VERSION}.jar && \
    curl -LO https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar && \
    mkdir -p /opt/flink/lib/hadoop && cd /opt/flink/lib/hadoop && \
    curl -LO https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-client-api/${HADOOP_VERSION}/hadoop-client-api-${HADOOP_VERSION}.jar && \
    curl -LO https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-client-runtime/${HADOOP_VERSION}/hadoop-client-runtime-${HADOOP_VERSION}.jar

COPY --from=build --chown=flink:flink /home/gradle/project/iceberg-avro-logical-types/build/libs/iceberg-avro-logical-types-${ICEBERG_VERSION}-all.jar /opt/flink/lib/iceberg/
COPY --from=build --chown=flink:flink /home/gradle/project/kafka-dynamic-iceberg-sink/build/libs/kafka-dynamic-iceberg-sink-all.jar /opt/flink/usrlib/
