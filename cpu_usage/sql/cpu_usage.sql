CREATE DATABASE homework;
\c homework
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE TABLE cpu_usage(
  ts    TIMESTAMPTZ,
  host  TEXT,
  usage DOUBLE PRECISION
);
SELECT create_hypertable('cpu_usage', 'ts');
\COPY cpu_usage FROM '/var/lib/postgresql/data/cpu_usage.csv' DELIMITER ',' CSV HEADER;
