FROM gradle:8.7-jdk17 AS builder

WORKDIR /src

COPY sources/temp-source-connector/ ./

RUN gradle --no-daemon clean jar

FROM confluentinc/cp-kafka-connect-base:8.1.0

ADD connectors-pack.tar.gz /usr/share/confluent-hub-components/

COPY --from=builder /src/build/libs/temp-source-connector-1.0.0.jar /usr/share/confluent-hub-components/temp-source-connector/
