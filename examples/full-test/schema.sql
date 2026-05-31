CREATE DATABASE IF NOT EXISTS cdr;

DROP TABLE IF EXISTS cdr.dns_cdr;
DROP TABLE IF EXISTS cdr.http_cdr;

CREATE TABLE cdr.dns_cdr
(
    event_time DateTime,
    src_ip IPv4,
    dst_ip IPv4,
    domain String,
    dns_type LowCardinality(String),
    response_code UInt16,
    probe_id LowCardinality(String),
    region LowCardinality(String),
    src_geo_country LowCardinality(String),
    src_geo_province LowCardinality(String),
    src_geo_city LowCardinality(String),
    src_geo_isp LowCardinality(String),
    dst_geo_country LowCardinality(String),
    dst_geo_province LowCardinality(String),
    dst_geo_city LowCardinality(String),
    dst_geo_isp LowCardinality(String),
    ingest_time DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_time, src_ip, domain);

CREATE TABLE cdr.http_cdr
(
    event_time DateTime,
    client_ip IPv4,
    server_ip IPv4,
    method LowCardinality(String),
    url String,
    status_code UInt16,
    bytes_sent UInt64,
    probe_id LowCardinality(String),
    region LowCardinality(String),
    client_geo_country LowCardinality(String),
    client_geo_province LowCardinality(String),
    client_geo_city LowCardinality(String),
    client_geo_isp LowCardinality(String),
    server_geo_country LowCardinality(String),
    server_geo_province LowCardinality(String),
    server_geo_city LowCardinality(String),
    server_geo_isp LowCardinality(String),
    ingest_time DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (event_time, client_ip, status_code);
