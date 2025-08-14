-- Flink Upsert-Kafka Connector Local Example with Datagen

-- Set execution mode to streaming
SET 'execution.runtime-mode' = 'streaming';

-- Set result mode to changelog for streaming results
SET
'sql-client.execution.result-mode' = 'changelog';

-- Set up the datagen source table to generate synthetic tenant data
CREATE TABLE TenantDataGen (
    tenant_id BIGINT,
    tentant_name STRING,
    email STRING,
    registration_date TIMESTAMP(3),
    loyalty_level STRING,
    country STRING,
    city STRING,
    tenant_segment STRING,
    update_time AS PROCTIME()
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '1000',
    'fields.tenant_id.min' = '1',
    'fields.tenant_id.max' = '10000',
    'fields.tentant_name.length' = '15',
    'fields.email.length' = '20',
    'fields.loyalty_level.length' = '1',
    'fields.country.length' = '3',
    'fields.city.length' = '10',
    'fields.tenant_segment.length' = '1'
);

-- Set up the Kafka sink table with upsert support
CREATE TABLE EnrichedTenantKafka (
    tenant_id BIGINT,
    tentant_name STRING,
    email STRING,
    loyalty_level STRING,
    country STRING,
    PRIMARY KEY (tenant_id) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'enriched_tenant_topic',
    'properties.bootstrap.servers' = 'localhost:9092',
    'key.format' = 'json',
    'value.format' = 'json'
);

-- Read from datagen, transform, and write to the Kafka sink
INSERT INTO EnrichedTenantKafka
SELECT
    tenant_id,
    tentant_name,
    email,
    loyalty_level,
    country
FROM TenantDataGen; 
