-- Flink Upsert-Kafka Connector Local Example with Datagen

-- Flink Join Benchmark: Binary vs Multi-way Joins
-- Set execution mode to streaming for continuous data generation
SET
'execution.runtime-mode' = 'streaming';
SET
'parallelism.default' = '1';
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
'execution.checkpointing.interval' = '10s';
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

SET 'state.backend.rocksdb.metrics.num-running-compactions' = 'true';
SET 'state.backend.rocksdb.metrics.compaction-pending' = 'true';
SET 'state.backend.rocksdb.metrics.estimate-pending-compaction-bytes' = 'true';
SET 'state.backend.rocksdb.metrics.mem-table-flush-pending' = 'true';
SET 'state.backend.rocksdb.metrics.cur-size-all-mem-tables' = 'true';
SET 'state.backend.rocksdb.metrics.estimate-live-data-size' = 'true';

-- Set up the Tenant Kafka sink table with upsert support
CREATE TABLE TenantKafka
(
    tenant_id   BIGINT,
    large_array ARRAY<STRING>,
    PRIMARY KEY (tenant_id) NOT ENFORCED
) WITH (
      'connector' = 'upsert-kafka',
      'topic' = 'tenant_topic_1kk',
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
      'topic' = 'suppliers_topic_1kk',
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
      'topic' = 'products_topic_1kk',
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
      'topic' = 'categories_topic_1kk',
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
      'topic' = 'orders_topic_1kk',
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
      'topic' = 'customers_topic_1kk',
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
      'topic' = 'warehouses_topic_1kk',
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
      'topic' = 'shipping_topic_1kk',
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
      'topic' = 'payment_topic_1kk',
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
      'topic' = 'inventory_topic_1kk',
      'properties.bootstrap.servers' = 'localhost:9092',
      'key.format' = 'json',
      'value.format' = 'json'
      );

-- Set up the Join Results Kafka sink table
CREATE TABLE JoinResultsMJ
(
    tenant_id         BIGINT,
    order_id          BIGINT,
    supplier_id       BIGINT,
    product_id        BIGINT,
    category_id       BIGINT,
    customer_id       BIGINT,
    warehouse_id      BIGINT,
    shipping_id       BIGINT,
    payment_id        BIGINT,
    inventory_id      BIGINT,
    tenant_large_array     ARRAY<STRING>,
    supplier_large_array   ARRAY<STRING>,
    product_large_array    ARRAY<STRING>,
    category_large_array    ARRAY<STRING>,
    order_large_array      ARRAY<STRING>,
    customer_large_array    ARRAY<STRING>,
    warehouse_large_array   ARRAY<STRING>,
    shipping_large_array   ARRAY<STRING>,
    payment_large_array    ARRAY<STRING>,
    inventory_large_array  ARRAY<STRING>
) WITH (
      'connector' = 'blackhole'

--         'connector' = 'upsert-kafka',
--       'topic' = 'join_results_mj_topic_1kk',
--       'properties.bootstrap.servers' = 'localhost:9092',
--       'key.format' = 'json',
--       'value.format' = 'json'

--         'connector' = 'kafka',
--         'topic' = 'join_results_mj_topic_1kk',
--         'properties.bootstrap.servers' = 'localhost:9092',
--         'format' = 'json'
      );

-- Set up the Join Results Kafka sink table
CREATE TABLE JoinResultsBinary
(
    tenant_id         BIGINT,
    order_id          BIGINT,
    supplier_id       BIGINT,
    product_id        BIGINT,
    category_id       BIGINT,
    customer_id       BIGINT,
    warehouse_id      BIGINT,
    shipping_id       BIGINT,
    payment_id        BIGINT,
    inventory_id      BIGINT,
    tenant_large_array     ARRAY<STRING>,
    supplier_large_array   ARRAY<STRING>,
    product_large_array    ARRAY<STRING>,
    category_large_array    ARRAY<STRING>,
    order_large_array      ARRAY<STRING>,
    customer_large_array    ARRAY<STRING>,
    warehouse_large_array   ARRAY<STRING>,
    shipping_large_array   ARRAY<STRING>,
    payment_large_array    ARRAY<STRING>,
    inventory_large_array  ARRAY<STRING>
) WITH (
      'connector' = 'blackhole'

--     'connector' = 'upsert-kafka',
--       'topic' = 'join_results_binary_topic_1kk',
--       'properties.bootstrap.servers' = 'localhost:9092',
--       'key.format' = 'json',
--       'value.format' = 'json'

--       'connector' = 'kafka',
--       'topic' = 'join_results_binary_topic_1kk',
--       'properties.bootstrap.servers' = 'localhost:9092',
--       'format' = 'json'
      );

-- 10-way Join: Using subqueries with explicit key preservation

-- Set parallelism for 10-way join
SET
'parallelism.default' = '10';
-- SET
-- 'table.optimizer.multi-join.enabled' = 'true';
--
-- INSERT INTO JoinResultsMJ
-- SELECT t.tenant_id                    as tenant_id,
--        o.order_id                     AS order_id,
--        COALESCE(s.supplier_id, -1)    AS supplier_id,
--        COALESCE(p.product_id, -1)     AS product_id,
--        COALESCE(c.category_id, -1)    AS category_id,
--        COALESCE(cust.customer_id, -1) AS customer_id,
--        COALESCE(w.warehouse_id, -1)   AS warehouse_id,
--        COALESCE(sh.shipping_id, -1)   AS shipping_id,
--        COALESCE(pay.payment_id, -1)   AS payment_id,
--        COALESCE(i.inventory_id, -1)   AS inventory_id,
--        t.large_array                  AS tenant_large_array,
--        s.large_array                  AS supplier_large_array,
--        p.large_array                  AS product_large_array,
--        c.large_array                  AS category_large_array,
--        o.large_array                  AS order_large_array,
--        cust.large_array               AS customer_large_array,
--        w.large_array                  AS warehouse_large_array,
--        sh.large_array                 AS shipping_large_array,
--        pay.large_array                AS payment_large_array,
--        i.large_array                  AS inventory_large_array
-- FROM OrdersKafka o
--          LEFT JOIN SuppliersKafka s ON o.tenant_id = s.tenant_id
--          LEFT JOIN ProductsKafka p ON o.tenant_id = p.tenant_id
--          LEFT JOIN CategoriesKafka c ON o.tenant_id = c.tenant_id
--          LEFT JOIN TenantKafka t ON o.tenant_id = t.tenant_id
--          LEFT JOIN CustomersKafka cust ON o.tenant_id = cust.tenant_id
--          LEFT JOIN WarehousesKafka w ON o.tenant_id = w.tenant_id
--          LEFT JOIN ShippingKafka sh ON o.tenant_id = sh.tenant_id
--          LEFT JOIN PaymentKafka pay ON o.tenant_id = pay.tenant_id
--          LEFT JOIN InventoryKafka i ON o.tenant_id = i.tenant_id;
/*WHERE
    CARDINALITY(t.large_array) > 0 OR
    CARDINALITY(s.large_array) > 0 OR
    CARDINALITY(p.large_array) > 0 OR
    CARDINALITY(c.large_array) > 0 OR
    CARDINALITY(o.large_array) > 0 OR
    CARDINALITY(cust.large_array) > 0 OR
    CARDINALITY(w.large_array) > 0 OR
    CARDINALITY(sh.large_array) > 0 OR
    CARDINALITY(pay.large_array) > 0 OR
    CARDINALITY(i.large_array) > 0;*/

SET
'table.optimizer.multi-join.enabled' = 'false';

-- 10-way Join: Alternative approach with window functions for key preservation
INSERT INTO JoinResultsBinary
SELECT t.tenant_id                    as tenant_id,
       o.order_id                     AS order_id,
       COALESCE(s.supplier_id, -1)    AS supplier_id,
       COALESCE(p.product_id, -1)     AS product_id,
       COALESCE(c.category_id, -1)    AS category_id,
       COALESCE(cust.customer_id, -1) AS customer_id,
       COALESCE(w.warehouse_id, -1)   AS warehouse_id,
       COALESCE(sh.shipping_id, -1)   AS shipping_id,
       COALESCE(pay.payment_id, -1)   AS payment_id,
       COALESCE(i.inventory_id, -1)   AS inventory_id,
       t.large_array                  AS tenant_large_array,
       s.large_array                  AS supplier_large_array,
       p.large_array                  AS product_large_array,
       c.large_array                  AS category_large_array,
       o.large_array                  AS order_large_array,
       cust.large_array               AS customer_large_array,
       w.large_array                  AS warehouse_large_array,
       sh.large_array                 AS shipping_large_array,
       pay.large_array                AS payment_large_array,
       i.large_array                  AS inventory_large_array
FROM OrdersKafka o
         LEFT JOIN (SELECT * FROM SuppliersKafka WHERE tenant_id < 500000) s ON o.tenant_id = s.tenant_id
         LEFT JOIN (SELECT * FROM ProductsKafka WHERE tenant_id < 500000) p ON o.tenant_id = p.tenant_id
         LEFT JOIN (SELECT * FROM CategoriesKafka WHERE tenant_id < 500000) c ON o.tenant_id = c.tenant_id
         LEFT JOIN (SELECT * FROM TenantKafka WHERE tenant_id < 500000) t ON o.tenant_id = t.tenant_id
         LEFT JOIN (SELECT * FROM CustomersKafka WHERE tenant_id < 500000) cust ON o.tenant_id = cust.tenant_id
         LEFT JOIN (SELECT * FROM WarehousesKafka WHERE tenant_id < 500000) w ON o.tenant_id = w.tenant_id
         LEFT JOIN (SELECT * FROM ShippingKafka WHERE tenant_id < 500000) sh ON o.tenant_id = sh.tenant_id
         LEFT JOIN (SELECT * FROM PaymentKafka WHERE tenant_id < 500000) pay ON o.tenant_id = pay.tenant_id
         LEFT JOIN (SELECT * FROM InventoryKafka WHERE tenant_id < 500000) i ON o.tenant_id = i.tenant_id
WHERE o.tenant_id < 500000;
