SET 'execution.checkpointing.interval' = '50 s';

CREATE TABLE temp_raw
(
    sensorId     STRING NULL,
    `timestamp`  TIMESTAMP(3) NULL,
    latitude     DOUBLE NULL,
    longitude    DOUBLE NULL,
    temperatureF DOUBLE NULL,
    WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'earth-temp',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-temp',
    'scan.startup.mode' = 'group-offsets',
    'properties.auto.offset.reset' = 'earliest',
    'format' = 'avro-confluent',
    'avro-confluent.schema-registry.url' = 'http://kafka-schema-registry:8081'
);

CREATE VIEW temp_strict AS
SELECT
    sensorId,
    `timestamp`,
    latitude,
    longitude,
    temperatureF
FROM temp_raw
WHERE sensorId IS NOT NULL
  AND `timestamp` IS NOT NULL
  AND latitude IS NOT NULL
  AND longitude IS NOT NULL
  AND temperatureF IS NOT NULL;

CREATE TABLE temp_avg (
    sensorId      STRING NOT NULL,
    window_start  TIMESTAMP(3) NOT NULL,
    window_end    TIMESTAMP(3) NOT NULL,
    latitude      DOUBLE NOT NULL,
    longitude     DOUBLE NOT NULL,
    temperatureF  DOUBLE NOT NULL
) WITH (
    'connector' = 'kafka',
    'topic' = 'earth-temp-avg',
    'properties.bootstrap.servers' = 'kafka:9092',

    'key.format' = 'raw',
    'key.fields' = 'sensorId',

    'format' = 'avro-confluent',
    'avro-confluent.schema-registry.url' = 'http://kafka-schema-registry:8081'
);

INSERT INTO temp_avg
SELECT
    sensorId,
    window_start,
    window_end,
    AVG(latitude)     AS latitude,
    AVG(longitude)    AS longitude,
    AVG(temperatureF) AS temperatureF
FROM TABLE(
    TUMBLE(TABLE temp_strict, DESCRIPTOR(`timestamp`), INTERVAL '1' MINUTE)
)
GROUP BY sensorId, window_start, window_end;
