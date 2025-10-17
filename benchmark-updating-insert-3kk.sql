-- Flink Upsert-Kafka Connector Local Example with Datagen

-- Flink Join Benchmark: Binary vs Multi-way Joins
-- Set execution mode to streaming for continuous data generation
SET
'execution.runtime-mode' = 'streaming';

-- Set result mode to changelog for streaming results
SET
'sql-client.execution.result-mode' = 'changelog';

SET
'rest.flamegraph.enabled' = 'true';

-- Set execution mode to local for development
--SET 'execution.target' = 'local';

-- Set state backend
SET
'state.backend' = 'rocksdb';
SET
'state.backend.incremental' = 'true';
SET
'state.checkpoints.dir' = 'file:///Users/gdemorais/qdev/flink2/build-target/flink-checkpoints';

SET
'execution.checkpointing.interval' = '30s';
SET
'execution.checkpointing.timeout' = '60min';
SET
'execution.checkpointing.min-pause' = '5s';
SET
'execution.checkpointing.max-concurrent-checkpoints' = '1';
SET
'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET
'execution.checkpointing.unaligned' = 'true';
-- SET
-- 'execution.checkpointing.aligned-checkpoint-timeout' = '1min';

-- Kafka consumer memory settings (these can be set via SQL)
SET
'table.connector.kafka.consumer.fetch.min.bytes' = '1';
SET
'table.connector.kafka.consumer.fetch.max.wait.ms' = '500';
SET
'table.connector.kafka.consumer.max.partition.fetch.bytes' = '1048576';
SET
'table.connector.kafka.consumer.receive.buffer.bytes' = '32768';
SET
'table.connector.kafka.consumer.send.buffer.bytes' = '131072';

SET
'parallelism.default' = '5';

--SET
--'table.optimizer.multi-join.enabled' = 'true';

-- Set up the datagen source table to generate synthetic tenant data
CREATE TABLE TenantDataGen
(
    tenant_id   BIGINT,
    large_array ARRAY<STRING>,
    update_time AS PROCTIME()
) WITH (
      'connector' = 'datagen',
      'rows-per-second' = '10000',
      'number-of-rows' = '3000000',
      'fields.tenant_id.min' = '1',
      'fields.tenant_id.max' = '3000000',
      'fields.large_array.length' = '1'
      );

-- Supplier dimension table with datagen source
CREATE TABLE SupplierDataGen
(
    tenant_id   BIGINT,
    supplier_id BIGINT,
    large_array ARRAY<STRING>,
    update_time AS PROCTIME()
) WITH (
      'connector' = 'datagen',
      'rows-per-second' = '10000',
      'number-of-rows' = '3000000',
      'fields.tenant_id.min' = '1',
      'fields.tenant_id.max' = '3000000',
      'fields.supplier_id.min' = '1',
      'fields.supplier_id.max' = '100000000',
      'fields.large_array.length' = '1'
      );

-- Product dimension table with datagen source
CREATE TABLE ProductDataGen
(
    tenant_id   BIGINT,
    product_id  BIGINT,
    large_array ARRAY<STRING>,
    update_time AS PROCTIME()
) WITH (
      'connector' = 'datagen',
      'rows-per-second' = '10000',
      'number-of-rows' = '3000000',
      'fields.tenant_id.min' = '1',
      'fields.tenant_id.max' = '3000000',
      'fields.product_id.min' = '1',
      'fields.product_id.max' = '100000000',
      'fields.large_array.length' = '1'
      );

-- Category dimension table with datagen source
CREATE TABLE CategoryDataGen
(
    tenant_id   BIGINT,
    category_id BIGINT,
    large_array ARRAY<STRING>,
    update_time AS PROCTIME()
) WITH (
      'connector' = 'datagen',
      'rows-per-second' = '10000',
      'number-of-rows' = '3000000',
      'fields.tenant_id.min' = '1',
      'fields.tenant_id.max' = '3000000',
      'fields.category_id.min' = '1',
      'fields.category_id.max' = '100000000',
      'fields.large_array.length' = '1'
      );

-- Orders table (fact table) with datagen source
CREATE TABLE OrderDataGen
(
    tenant_id   BIGINT,
    order_id    BIGINT,
    large_array ARRAY<STRING>,
    update_time AS PROCTIME()
) WITH (
      'connector' = 'datagen',
      'rows-per-second' = '10000',
      'number-of-rows' = '3000000',
      'fields.tenant_id.min' = '1',
      'fields.tenant_id.max' = '3000000',
      'fields.order_id.min' = '1',
      'fields.order_id.max' = '100000000',
      'fields.large_array.length' = '1'
      );

-- Customer dimension table with datagen source
CREATE TABLE CustomerDataGen
(
    tenant_id   BIGINT,
    customer_id BIGINT,
    large_array ARRAY<STRING>,
    update_time AS PROCTIME()
) WITH (
      'connector' = 'datagen',
      'rows-per-second' = '10000',
      'number-of-rows' = '3000000',
      'fields.tenant_id.min' = '1',
      'fields.tenant_id.max' = '3000000',
      'fields.customer_id.min' = '1',
      'fields.customer_id.max' = '100000000',
      'fields.large_array.length' = '1'
      );

-- Warehouse dimension table with datagen source
CREATE TABLE WarehouseDataGen
(
    tenant_id    BIGINT,
    warehouse_id BIGINT,
    large_array  ARRAY<STRING>,
    update_time AS PROCTIME()
) WITH (
      'connector' = 'datagen',
      'rows-per-second' = '10000',
      'number-of-rows' = '3000000',
      'fields.tenant_id.min' = '1',
      'fields.tenant_id.max' = '3000000',
      'fields.warehouse_id.min' = '1',
      'fields.warehouse_id.max' = '100000000',
      'fields.large_array.length' = '1'
      );

-- Shipping dimension table with datagen source
CREATE TABLE ShippingDataGen
(
    tenant_id   BIGINT,
    shipping_id BIGINT,
    large_array ARRAY<STRING>,
    update_time AS PROCTIME()
) WITH (
      'connector' = 'datagen',
      'rows-per-second' = '10000',
      'number-of-rows' = '3000000',
      'fields.tenant_id.min' = '1',
      'fields.tenant_id.max' = '3000000',
      'fields.shipping_id.min' = '1',
      'fields.shipping_id.max' = '100000000',
      'fields.large_array.length' = '1'
      );

-- Payment dimension table with datagen source
CREATE TABLE PaymentDataGen
(
    tenant_id   BIGINT,
    payment_id  BIGINT,
    large_array ARRAY<STRING>,
    update_time AS PROCTIME()
) WITH (
      'connector' = 'datagen',
      'rows-per-second' = '10000',
      'number-of-rows' = '3000000',
      'fields.tenant_id.min' = '1',
      'fields.tenant_id.max' = '3000000',
      'fields.payment_id.min' = '1',
      'fields.payment_id.max' = '100000000',
      'fields.large_array.length' = '1'
      );

-- Inventory dimension table with datagen source
CREATE TABLE InventoryDataGen
(
    tenant_id    BIGINT,
    inventory_id BIGINT,
    large_array  ARRAY<STRING>,
    update_time AS PROCTIME()
) WITH (
      'connector' = 'datagen',
      'rows-per-second' = '10000',
      'number-of-rows' = '3000000',
      'fields.tenant_id.min' = '1',
      'fields.tenant_id.max' = '3000000',
      'fields.inventory_id.min' = '1',
      'fields.inventory_id.max' = '100000000',
      'fields.large_array.length' = '1'
      );

-- Set up the Tenant Kafka sink table with upsert support
CREATE TABLE TenantKafka
(
    tenant_id   BIGINT,
    large_array ARRAY<STRING>,
    PRIMARY KEY (tenant_id) NOT ENFORCED
) WITH (
      'connector' = 'upsert-kafka',
      'topic' = 'tenant_topic_3kk',
      'properties.bootstrap.servers' = 'localhost:9092',
      'key.format' = 'json',
      'value.format' = 'json'
      );

-- Set up the Suppliers Kafka sink table with upsert support
CREATE TABLE SuppliersKafka
(
    tenant_id   BIGINT,
    supplier_id BIGINT,
    large_array ARRAY<STRING>,
    PRIMARY KEY (supplier_id) NOT ENFORCED
) WITH (
      'connector' = 'upsert-kafka',
      'topic' = 'suppliers_topic_3kk',
      'properties.bootstrap.servers' = 'localhost:9092',
      'key.format' = 'json',
      'value.format' = 'json'
      );

-- Set up the Products Kafka sink table with upsert support
CREATE TABLE ProductsKafka
(
    tenant_id   BIGINT,
    product_id  BIGINT,
    large_array ARRAY<STRING>,
    PRIMARY KEY (product_id) NOT ENFORCED
) WITH (
      'connector' = 'upsert-kafka',
      'topic' = 'products_topic_3kk',
      'properties.bootstrap.servers' = 'localhost:9092',
      'key.format' = 'json',
      'value.format' = 'json'
      );

-- Set up the Categories Kafka sink table with upsert support
CREATE TABLE CategoriesKafka
(
    tenant_id   BIGINT,
    category_id BIGINT,
    large_array ARRAY<STRING>,
    PRIMARY KEY (category_id) NOT ENFORCED
) WITH (
      'connector' = 'upsert-kafka',
      'topic' = 'categories_topic_3kk',
      'properties.bootstrap.servers' = 'localhost:9092',
      'key.format' = 'json',
      'value.format' = 'json'
      );

-- Set up the Orders Kafka sink table with upsert support
CREATE TABLE OrdersKafka
(
    tenant_id   BIGINT,
    order_id    BIGINT,
    large_array ARRAY<STRING>,
    PRIMARY KEY (tenant_id, order_id) NOT ENFORCED
) WITH (
      'connector' = 'upsert-kafka',
      'topic' = 'orders_topic_3kk',
      'properties.bootstrap.servers' = 'localhost:9092',
      'key.format' = 'json',
      'value.format' = 'json'
      );

-- Set up the Customers Kafka sink table with upsert support
CREATE TABLE CustomersKafka
(
    tenant_id   BIGINT,
    customer_id BIGINT,
    large_array ARRAY<STRING>,
    PRIMARY KEY (customer_id) NOT ENFORCED
) WITH (
      'connector' = 'upsert-kafka',
      'topic' = 'customers_topic_3kk',
      'properties.bootstrap.servers' = 'localhost:9092',
      'key.format' = 'json',
      'value.format' = 'json'
      );

-- Set up the Warehouses Kafka sink table with upsert support
CREATE TABLE WarehousesKafka
(
    tenant_id    BIGINT,
    warehouse_id BIGINT,
    large_array  ARRAY<STRING>,
    PRIMARY KEY (warehouse_id) NOT ENFORCED
) WITH (
      'connector' = 'upsert-kafka',
      'topic' = 'warehouses_topic_3kk',
      'properties.bootstrap.servers' = 'localhost:9092',
      'key.format' = 'json',
      'value.format' = 'json'
      );

-- Set up the Shipping Kafka sink table with upsert support
CREATE TABLE ShippingKafka
(
    tenant_id   BIGINT,
    shipping_id BIGINT,
    large_array ARRAY<STRING>,
    PRIMARY KEY (shipping_id) NOT ENFORCED
) WITH (
      'connector' = 'upsert-kafka',
      'topic' = 'shipping_topic_3kk',
      'properties.bootstrap.servers' = 'localhost:9092',
      'key.format' = 'json',
      'value.format' = 'json'
      );

-- Set up the Payment Kafka sink table with upsert support
CREATE TABLE PaymentKafka
(
    tenant_id   BIGINT,
    payment_id  BIGINT,
    large_array ARRAY<STRING>,
    PRIMARY KEY (payment_id) NOT ENFORCED
) WITH (
      'connector' = 'upsert-kafka',
      'topic' = 'payment_topic_3kk',
      'properties.bootstrap.servers' = 'localhost:9092',
      'key.format' = 'json',
      'value.format' = 'json'
      );

-- Set up the Inventory Kafka sink table with upsert support
CREATE TABLE InventoryKafka
(
    tenant_id    BIGINT,
    inventory_id BIGINT,
    large_array  ARRAY<STRING>,
    PRIMARY KEY (inventory_id) NOT ENFORCED
) WITH (
      'connector' = 'upsert-kafka',
      'topic' = 'inventory_topic_3kk',
      'properties.bootstrap.servers' = 'localhost:9092',
      'key.format' = 'json',
      'value.format' = 'json'
      );

-- Set up the Join Results Kafka sink table
CREATE TABLE JoinResultsMJ
(
    tenant_id    BIGINT,
    order_id     BIGINT,
    supplier_id  BIGINT,
    product_id   BIGINT,
    category_id  BIGINT,
    customer_id  BIGINT,
    warehouse_id BIGINT,
    shipping_id  BIGINT,
    payment_id   BIGINT,
    inventory_id BIGINT
) WITH (
      'connector' = 'blackhole'

--         'connector' = 'upsert-kafka',
--       'topic' = 'join_results_mj_topic_3kk',
--       'properties.bootstrap.servers' = 'localhost:9092',
--       'key.format' = 'json',
--       'value.format' = 'json'

--         'connector' = 'kafka',
--         'topic' = 'join_results_mj_topic_3kk',
--         'properties.bootstrap.servers' = 'localhost:9092',
--         'format' = 'json'
      );

-- Set up the Join Results Kafka sink table
CREATE TABLE JoinResultsBinary
(
    tenant_id    BIGINT,
    order_id     BIGINT,
    supplier_id  BIGINT,
    product_id   BIGINT,
    category_id  BIGINT,
    customer_id  BIGINT,
    warehouse_id BIGINT,
    shipping_id  BIGINT,
    payment_id   BIGINT,
    inventory_id BIGINT
) WITH (
       'connector' = 'blackhole'

--     'connector' = 'upsert-kafka',
--       'topic' = 'join_results_binary_topic_3kk',
--       'properties.bootstrap.servers' = 'localhost:9092',
--       'key.format' = 'json',
--       'value.format' = 'json'

--       'connector' = 'kafka',
--       'topic' = 'join_results_binary_topic_3kk',
--       'properties.bootstrap.servers' = 'localhost:9092',
--       'format' = 'json'
      );

-- 10-way Join: Using subqueries with explicit key preservation

-- Insert tenant data into Kafka
INSERT INTO TenantKafka
SELECT tenant_id,
       large_array
FROM TenantDataGen;

-- Insert supplier data into Kafka
INSERT INTO SuppliersKafka
SELECT tenant_id,
       supplier_id,
       large_array
FROM SupplierDataGen;

-- Insert product data into Kafka
INSERT INTO ProductsKafka
SELECT tenant_id,
       product_id,
       large_array
FROM ProductDataGen;

-- Insert category data into Kafka
INSERT INTO CategoriesKafka
SELECT tenant_id,
       category_id,
       large_array
FROM CategoryDataGen;

-- Insert order data into Kafka
INSERT INTO OrdersKafka
SELECT tenant_id,
       order_id,
       large_array
FROM OrderDataGen;

-- Insert customer data into Kafka
INSERT INTO CustomersKafka
SELECT tenant_id,
       customer_id,
       large_array
FROM CustomerDataGen;

-- Insert warehouse data into Kafka
INSERT INTO WarehousesKafka
SELECT tenant_id,
       warehouse_id,
       large_array
FROM WarehouseDataGen;

-- Insert shipping data into Kafka
INSERT INTO ShippingKafka
SELECT tenant_id,
       shipping_id,
       large_array
FROM ShippingDataGen;

-- Insert payment data into Kafka
INSERT INTO PaymentKafka
SELECT tenant_id,
       payment_id,
       large_array
FROM PaymentDataGen;

-- Insert inventory data into Kafka
INSERT INTO InventoryKafka
SELECT tenant_id,
       inventory_id,
       large_array
FROM InventoryDataGen;
