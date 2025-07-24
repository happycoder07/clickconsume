CREATE DATABASE IF NOT EXISTS logs;

-- Target table
CREATE TABLE IF NOT EXISTS logs.ipfix_entry (
    flowStartMilliseconds DateTime,
    flowEndMilliseconds DateTime,
    octetTotalCount UInt32,
    packetTotalCount UInt32,
    destinationIPv4Address String,
    sourceIPv4Address String,
    destinationTransportPort UInt16,
    sourceTransportPort UInt16,
    protocolIdentifier String,
    flowEndReason UInt8,
    httpUserAgent String,
    httpGet String,
    httpHost String,
    httpResponse String
) ENGINE = MergeTree()
ORDER BY flowStartMilliseconds;

-- Kafka engine table
CREATE TABLE IF NOT EXISTS logs.ipfix_kafka (
    value String
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'ipfix_logs',
    kafka_group_name = 'clickhouse_group',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

-- Materialized view to parse and insert data
CREATE MATERIALIZED VIEW IF NOT EXISTS logs.ipfix_mv TO logs.ipfix_entry AS
SELECT
    parseDateTimeBestEffortOrNull(JSONExtractString(value, 'iana:flowStartMilliseconds')) AS flowStartMilliseconds,
    parseDateTimeBestEffortOrNull(JSONExtractString(value, 'iana:flowEndMilliseconds')) AS flowEndMilliseconds,
    toUInt32OrZero(JSONExtractString(value, 'iana:octetTotalCount')) AS octetTotalCount,
    toUInt32OrZero(JSONExtractString(value, 'iana:packetTotalCount')) AS packetTotalCount,
    JSONExtractString(value, 'iana:destinationIPv4Address') AS destinationIPv4Address,
    JSONExtractString(value, 'iana:sourceIPv4Address') AS sourceIPv4Address,
    toUInt16OrZero(JSONExtractString(value, 'iana:destinationTransportPort')) AS destinationTransportPort,
    toUInt16OrZero(JSONExtractString(value, 'iana:sourceTransportPort')) AS sourceTransportPort,
    JSONExtractString(value, 'iana:protocolIdentifier') AS protocolIdentifier,
    toUInt8OrZero(JSONExtractString(value, 'iana:flowEndReason')) AS flowEndReason,
    '' AS httpUserAgent,
    '' AS httpGet,
    '' AS httpHost,
    '' AS httpResponse
FROM logs.ipfix_kafka; 