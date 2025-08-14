-- Simple Updating Join Example with COALESCE
-- This demonstrates how LEFT JOIN with COALESCE works in a streaming context

-- Set execution mode to streaming
SET 'execution.runtime-mode' = 'streaming';

-- Set result mode to changelog for streaming results
SET 'sql-client.execution.result-mode' = 'changelog';

-- Simple user table with datagen
CREATE TABLE UserDataGen (
    user_id BIGINT,
    user_name STRING,
    email STRING,
    update_time AS PROCTIME()
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '5',
    'fields.user_id.min' = '1',
    'fields.user_id.max' = '10',
    'fields.user_name.length' = '8',
    'fields.email.length' = '12'
);

-- Simple order table with datagen
CREATE TABLE OrderDataGen (
    user_id BIGINT,
    order_id BIGINT,
    product_name STRING,
    amount DECIMAL(10,2),
    update_time AS PROCTIME()
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '3',
    'fields.user_id.min' = '1',
    'fields.user_id.max' = '15', -- Some users won't have orders
    'fields.order_id.min' = '1',
    'fields.order_id.max' = '1000',
    'fields.product_name.length' = '10',
    'fields.amount.min' = '10.00',
    'fields.amount.max' = '500.00'
);

-- User Kafka sink
CREATE TABLE UserKafka (
    user_id BIGINT,
    user_name STRING,
    email STRING,
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'user_topic',
    'properties.bootstrap.servers' = 'localhost:9092',
    'key.format' = 'json',
    'value.format' = 'json'
);

-- Order Kafka sink
CREATE TABLE OrderKafka (
    user_id BIGINT,
    order_id BIGINT,
    product_name STRING,
    amount DECIMAL(10,2),
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'order_topic',
    'properties.bootstrap.servers' = 'localhost:9092',
    'key.format' = 'json',
    'value.format' = 'json'
);

-- Join Results sink with COALESCE
CREATE TABLE JoinResults (
    user_id BIGINT,
    user_name STRING,
    email STRING,
    order_id BIGINT,
    product_name STRING,
    amount DECIMAL(10,2),
    PRIMARY KEY (user_id, order_id) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'join_results_topic',
    'properties.bootstrap.servers' = 'localhost:9092',
    'key.format' = 'json',
    'value.format' = 'json'
);

-- Insert user data into Kafka
INSERT INTO UserKafka
SELECT
    user_id,
    user_name,
    email
FROM UserDataGen;

-- Insert order data into Kafka
INSERT INTO OrderKafka
SELECT
    user_id,
    order_id,
    product_name,
    amount
FROM OrderDataGen;

-- Join with COALESCE to handle NULLs from LEFT JOIN
INSERT INTO JoinResults
SELECT
    u.user_id,
    u.user_name,
    u.email,
    COALESCE(o.order_id, -1) AS order_id,
    COALESCE(o.product_name, 'NO_ORDER') AS product_name,
    COALESCE(o.amount, 0.00) AS amount
FROM UserKafka u
LEFT JOIN OrderKafka o ON u.user_id = o.user_id; 
