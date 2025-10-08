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
'execution.checkpointing.interval' = '30s';
SET
'execution.checkpointing.timeout' = '10min';
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

--SET
--'table.optimizer.multi-join.enabled' = 'true';

-- Set up the datagen source table to generate synthetic tenant data
CREATE TABLE TenantDataGen
(
    tenant_id         BIGINT,
    tentant_name      STRING,
    email             STRING,
    registration_date TIMESTAMP(3),
    loyalty_level     STRING,
    country           STRING,
    city              STRING,
    tenant_segment    STRING,
    large_array       ARRAY<STRING>,
    update_time AS PROCTIME()
) WITH (
      'connector' = 'datagen',
      'rows-per-second' = '1000',
      -- 'number-of-rows' = '100',
      'fields.tenant_id.min' = '1',
      'fields.tenant_id.max' = '1000',
      'fields.tentant_name.length' = '15',
      'fields.email.length' = '20',
      'fields.loyalty_level.length' = '1',
      'fields.country.length' = '3',
      'fields.city.length' = '10',
      'fields.tenant_segment.length' = '1',
      'fields.large_array.length' = '1'
      );

-- Supplier dimension table with datagen source
CREATE TABLE SupplierDataGen
(
    tenant_id         BIGINT,
    supplier_id       BIGINT,
    supplier_name     STRING,
    contact_person    STRING,
    phone             STRING,
    address           STRING,
    rating            DECIMAL(3, 2),
    active_since      TIMESTAMP(3),
    supplier_type     STRING,
    reliability_score DECIMAL(3, 2),
    large_array       ARRAY<STRING>,
    update_time AS PROCTIME()
) WITH (
      'connector' = 'datagen',
      'rows-per-second' = '1000',
      -- 'number-of-rows' = '100',
      'fields.tenant_id.min' = '1',
      'fields.tenant_id.max' = '1000',
      'fields.supplier_id.min' = '1',
      'fields.supplier_id.max' = '100000000',
      'fields.supplier_name.length' = '15',
      'fields.contact_person.length' = '12',
      'fields.phone.length' = '10',
      'fields.address.length' = '25',
      'fields.rating.min' = '1.0',
      'fields.rating.max' = '5.0',
      'fields.supplier_type.length' = '1',
      'fields.reliability_score.min' = '0.5',
      'fields.reliability_score.max' = '1.0',
      'fields.large_array.length' = '1'
      );

-- Product dimension table with datagen source
CREATE TABLE ProductDataGen
(
    tenant_id      BIGINT,
    product_id     BIGINT,
    product_name   STRING,
    category_id    BIGINT,
    brand          STRING,
    supplier_id    BIGINT,
    cost           DECIMAL(10, 2),
    weight         DECIMAL(8, 3),
    product_rating DECIMAL(3, 2),
    is_active      BOOLEAN,
    large_array    ARRAY<STRING>,
    update_time AS PROCTIME()
) WITH (
      'connector' = 'datagen',
      'rows-per-second' = '1000',
      -- 'number-of-rows' = '100',
      'fields.tenant_id.min' = '1',
      'fields.tenant_id.max' = '1000',
      'fields.product_id.min' = '1',
      'fields.product_id.max' = '100000000',
      'fields.product_name.length' = '20',
      'fields.category_id.min' = '1',
      'fields.category_id.max' = '1000',
      'fields.brand.length' = '8',
      'fields.supplier_id.min' = '1',
      'fields.supplier_id.max' = '500',
      'fields.cost.min' = '5.00',
      'fields.cost.max' = '500.00',
      'fields.weight.min' = '0.1',
      'fields.weight.max' = '10.0',
      'fields.product_rating.min' = '1.0',
      'fields.product_rating.max' = '5.0',
      'fields.large_array.length' = '1'
      );

-- Category dimension table with datagen source
CREATE TABLE CategoryDataGen
(
    tenant_id       BIGINT,
    category_id     BIGINT,
    category_name   STRING,
    description     STRING,
    parent_category STRING,
    is_active       BOOLEAN,
    created_date    TIMESTAMP(3),
    category_level  INT,
    display_order   INT,
    large_array     ARRAY<STRING>,
    update_time AS PROCTIME()
) WITH (
      'connector' = 'datagen',
      'rows-per-second' = '1000',
      -- 'number-of-rows' = '100',
      'fields.tenant_id.min' = '1',
      'fields.tenant_id.max' = '1000',
      'fields.category_id.min' = '1',
      'fields.category_id.max' = '100000000',
      'fields.category_name.length' = '12',
      'fields.description.length' = '30',
      'fields.parent_category.length' = '2',
      'fields.category_level.min' = '1',
      'fields.category_level.max' = '3',
      'fields.display_order.min' = '1',
      'fields.display_order.max' = '100',
      'fields.large_array.length' = '1'
      );

-- Orders table (fact table) with datagen source
CREATE TABLE OrderDataGen
(
    tenant_id      BIGINT,
    order_id       BIGINT,
    product_id     BIGINT,
    supplier_id    BIGINT,
    category_id    BIGINT,
    quantity       INT,
    unit_price     DECIMAL(10, 2),
    order_time     TIMESTAMP(3),
    region         STRING,
    payment_method STRING,
    order_status   STRING,
    large_array    ARRAY<STRING>,
    update_time AS PROCTIME()
) WITH (
      'connector' = 'datagen',
      'rows-per-second' = '1000',
      -- 'number-of-rows' = '100',
      'fields.tenant_id.min' = '1',
      'fields.tenant_id.max' = '1000',
      'fields.order_id.min' = '1',
      'fields.order_id.max' = '100000',
      'fields.product_id.min' = '1',
      'fields.product_id.max' = '5000',
      'fields.supplier_id.min' = '1',
      'fields.supplier_id.max' = '500',
      'fields.category_id.min' = '1',
      'fields.category_id.max' = '1000',
      'fields.quantity.min' = '1',
      'fields.quantity.max' = '10',
      'fields.unit_price.min' = '10.00',
      'fields.unit_price.max' = '1000.00',
      'fields.region.length' = '3',
      'fields.payment_method.length' = '2',
      'fields.order_status.length' = '1',
      'fields.large_array.length' = '1'
      );

-- Customer dimension table with datagen source
CREATE TABLE CustomerDataGen
(
    tenant_id                BIGINT,
    customer_id              BIGINT,
    customer_name            STRING,
    email                    STRING,
    phone                    STRING,
    address                  STRING,
    customer_type            STRING,
    registration_date        TIMESTAMP(3),
    loyalty_points           INT,
    preferred_payment_method STRING,
    large_array              ARRAY<STRING>,
    update_time AS PROCTIME()
) WITH (
      'connector' = 'datagen',
      'rows-per-second' = '1000',
      -- 'number-of-rows' = '100',
      'fields.tenant_id.min' = '1',
      'fields.tenant_id.max' = '1000',
      'fields.customer_id.min' = '1',
      'fields.customer_id.max' = '100000000',
      'fields.customer_name.length' = '15',
      'fields.email.length' = '20',
      'fields.phone.length' = '10',
      'fields.address.length' = '25',
      'fields.customer_type.length' = '1',
      'fields.loyalty_points.min' = '0',
      'fields.loyalty_points.max' = '10000',
      'fields.preferred_payment_method.length' = '2',
      'fields.large_array.length' = '1'
      );

-- Warehouse dimension table with datagen source
CREATE TABLE WarehouseDataGen
(
    tenant_id         BIGINT,
    warehouse_id      BIGINT,
    warehouse_name    STRING,
    location          STRING,
    capacity          INT,
    manager_name      STRING,
    contact_phone     STRING,
    warehouse_type    STRING,
    operational_hours STRING,
    efficiency_rating DECIMAL(3, 2),
    large_array       ARRAY<STRING>,
    update_time AS PROCTIME()
) WITH (
      'connector' = 'datagen',
      'rows-per-second' = '1000',
      -- 'number-of-rows' = '100',
      'fields.tenant_id.min' = '1',
      'fields.tenant_id.max' = '1000',
      'fields.warehouse_id.min' = '1',
      'fields.warehouse_id.max' = '100000000',
      'fields.warehouse_name.length' = '15',
      'fields.location.length' = '20',
      'fields.capacity.min' = '1000',
      'fields.capacity.max' = '50000',
      'fields.manager_name.length' = '12',
      'fields.contact_phone.length' = '10',
      'fields.warehouse_type.length' = '1',
      'fields.operational_hours.length' = '10',
      'fields.efficiency_rating.min' = '0.5',
      'fields.efficiency_rating.max' = '1.0',
      'fields.large_array.length' = '1'
      );

-- Shipping dimension table with datagen source
CREATE TABLE ShippingDataGen
(
    tenant_id           BIGINT,
    shipping_id         BIGINT,
    shipping_method     STRING,
    carrier_name        STRING,
    delivery_time_days  INT,
    shipping_cost       DECIMAL(8, 2),
    tracking_enabled    BOOLEAN,
    insurance_available BOOLEAN,
    max_weight          DECIMAL(8, 3),
    service_level       STRING,
    large_array         ARRAY<STRING>,
    update_time AS PROCTIME()
) WITH (
      'connector' = 'datagen',
      'rows-per-second' = '1000',
      -- 'number-of-rows' = '100',
      'fields.tenant_id.min' = '1',
      'fields.tenant_id.max' = '1000',
      'fields.shipping_id.min' = '1',
      'fields.shipping_id.max' = '100000000',
      'fields.shipping_method.length' = '10',
      'fields.carrier_name.length' = '12',
      'fields.delivery_time_days.min' = '1',
      'fields.delivery_time_days.max' = '30',
      'fields.shipping_cost.min' = '5.00',
      'fields.shipping_cost.max' = '100.00',
      'fields.max_weight.min' = '0.5',
      'fields.max_weight.max' = '50.0',
      'fields.service_level.length' = '1',
      'fields.large_array.length' = '1'
      );

-- Payment dimension table with datagen source
CREATE TABLE PaymentDataGen
(
    tenant_id               BIGINT,
    payment_id              BIGINT,
    payment_method          STRING,
    payment_provider        STRING,
    transaction_fee         DECIMAL(5, 2),
    processing_time_minutes INT,
    security_level          STRING,
    supported_currencies    STRING,
    max_transaction_amount  DECIMAL(10, 2),
    is_active               BOOLEAN,
    large_array             ARRAY<STRING>,
    update_time AS PROCTIME()
) WITH (
      'connector' = 'datagen',
      'rows-per-second' = '1000',
      -- 'number-of-rows' = '100',
      'fields.tenant_id.min' = '1',
      'fields.tenant_id.max' = '1000',
      'fields.payment_id.min' = '1',
      'fields.payment_id.max' = '100000000',
      'fields.payment_method.length' = '10',
      'fields.payment_provider.length' = '12',
      'fields.transaction_fee.min' = '0.10',
      'fields.transaction_fee.max' = '5.00',
      'fields.processing_time_minutes.min' = '1',
      'fields.processing_time_minutes.max' = '60',
      'fields.security_level.length' = '1',
      'fields.supported_currencies.length' = '10',
      'fields.max_transaction_amount.min' = '100.00',
      'fields.max_transaction_amount.max' = '10000.00',
      'fields.large_array.length' = '1'
      );

-- Inventory dimension table with datagen source
CREATE TABLE InventoryDataGen
(
    tenant_id         BIGINT,
    inventory_id      BIGINT,
    product_id        BIGINT,
    warehouse_id      BIGINT,
    quantity_on_hand  INT,
    reorder_level     INT,
    reorder_quantity  INT,
    last_restock_date TIMESTAMP(3),
    expiry_date       TIMESTAMP(3),
    storage_location  STRING,
    large_array       ARRAY<STRING>,
    update_time AS PROCTIME()
) WITH (
      'connector' = 'datagen',
      'rows-per-second' = '1000',
      -- 'number-of-rows' = '100',
      'fields.tenant_id.min' = '1',
      'fields.tenant_id.max' = '1000',
      'fields.inventory_id.min' = '1',
      'fields.inventory_id.max' = '100000000',
      'fields.product_id.min' = '1',
      'fields.product_id.max' = '5000',
      'fields.warehouse_id.min' = '1',
      'fields.warehouse_id.max' = '100',
      'fields.quantity_on_hand.min' = '0',
      'fields.quantity_on_hand.max' = '1000',
      'fields.reorder_level.min' = '10',
      'fields.reorder_level.max' = '100',
      'fields.reorder_quantity.min' = '50',
      'fields.reorder_quantity.max' = '500',
      'fields.storage_location.length' = '8',
      'fields.large_array.length' = '1'
      );

-- Set up the Tenant Kafka sink table with upsert support
CREATE TABLE TenantKafka
(
    tenant_id         BIGINT,
    tentant_name      STRING,
    email             STRING,
    registration_date TIMESTAMP(3),
    loyalty_level     STRING,
    country           STRING,
    city              STRING,
    tenant_segment    STRING,
    large_array       ARRAY<STRING>,
    PRIMARY KEY (tenant_id) NOT ENFORCED
) WITH (
      'connector' = 'upsert-kafka',
      'topic' = 'tenant_topic',
      'properties.bootstrap.servers' = 'localhost:9092',
      'key.format' = 'json',
      'value.format' = 'json'
      );

-- Set up the Suppliers Kafka sink table with upsert support
CREATE TABLE SuppliersKafka
(
    tenant_id         BIGINT,
    supplier_id       BIGINT,
    supplier_name     STRING,
    contact_person    STRING,
    phone             STRING,
    address           STRING,
    rating            DECIMAL(3, 2),
    active_since      TIMESTAMP(3),
    supplier_type     STRING,
    reliability_score DECIMAL(3, 2),
    large_array       ARRAY<STRING>,
    PRIMARY KEY (supplier_id) NOT ENFORCED
) WITH (
      'connector' = 'upsert-kafka',
      'topic' = 'suppliers_topic',
      'properties.bootstrap.servers' = 'localhost:9092',
      'key.format' = 'json',
      'value.format' = 'json'
      );

-- Set up the Products Kafka sink table with upsert support
CREATE TABLE ProductsKafka
(
    tenant_id      BIGINT,
    product_id     BIGINT,
    product_name   STRING,
    category_id    BIGINT,
    brand          STRING,
    supplier_id    BIGINT,
    cost           DECIMAL(10, 2),
    weight         DECIMAL(8, 3),
    product_rating DECIMAL(3, 2),
    is_active      BOOLEAN,
    large_array    ARRAY<STRING>,
    PRIMARY KEY (product_id) NOT ENFORCED
) WITH (
      'connector' = 'upsert-kafka',
      'topic' = 'products_topic',
      'properties.bootstrap.servers' = 'localhost:9092',
      'key.format' = 'json',
      'value.format' = 'json'
      );

-- Set up the Categories Kafka sink table with upsert support
CREATE TABLE CategoriesKafka
(
    tenant_id       BIGINT,
    category_id     BIGINT,
    category_name   STRING,
    description     STRING,
    parent_category STRING,
    is_active       BOOLEAN,
    created_date    TIMESTAMP(3),
    category_level  INT,
    display_order   INT,
    large_array     ARRAY<STRING>,
    PRIMARY KEY (category_id) NOT ENFORCED
) WITH (
      'connector' = 'upsert-kafka',
      'topic' = 'categories_topic',
      'properties.bootstrap.servers' = 'localhost:9092',
      'key.format' = 'json',
      'value.format' = 'json'
      );

-- Set up the Orders Kafka sink table with upsert support
CREATE TABLE OrdersKafka
(
    tenant_id      BIGINT,
    order_id       BIGINT,
    product_id     BIGINT,
    supplier_id    BIGINT,
    category_id    BIGINT,
    quantity       INT,
    unit_price     DECIMAL(10, 2),
    order_time     TIMESTAMP(3),
    region         STRING,
    payment_method STRING,
    order_status   STRING,
    large_array    ARRAY<STRING>,
    PRIMARY KEY (tenant_id, order_id) NOT ENFORCED
) WITH (
      'connector' = 'upsert-kafka',
      'topic' = 'orders_topic',
      'properties.bootstrap.servers' = 'localhost:9092',
      'key.format' = 'json',
      'value.format' = 'json'
      );

-- Set up the Customers Kafka sink table with upsert support
CREATE TABLE CustomersKafka
(
    tenant_id                BIGINT,
    customer_id              BIGINT,
    customer_name            STRING,
    email                    STRING,
    phone                    STRING,
    address                  STRING,
    customer_type            STRING,
    registration_date        TIMESTAMP(3),
    loyalty_points           INT,
    preferred_payment_method STRING,
    large_array              ARRAY<STRING>,
    PRIMARY KEY (customer_id) NOT ENFORCED
) WITH (
      'connector' = 'upsert-kafka',
      'topic' = 'customers_topic',
      'properties.bootstrap.servers' = 'localhost:9092',
      'key.format' = 'json',
      'value.format' = 'json'
      );

-- Set up the Warehouses Kafka sink table with upsert support
CREATE TABLE WarehousesKafka
(
    tenant_id         BIGINT,
    warehouse_id      BIGINT,
    warehouse_name    STRING,
    location          STRING,
    capacity          INT,
    manager_name      STRING,
    contact_phone     STRING,
    warehouse_type    STRING,
    operational_hours STRING,
    efficiency_rating DECIMAL(3, 2),
    large_array       ARRAY<STRING>,
    PRIMARY KEY (warehouse_id) NOT ENFORCED
) WITH (
      'connector' = 'upsert-kafka',
      'topic' = 'warehouses_topic',
      'properties.bootstrap.servers' = 'localhost:9092',
      'key.format' = 'json',
      'value.format' = 'json'
      );

-- Set up the Shipping Kafka sink table with upsert support
CREATE TABLE ShippingKafka
(
    tenant_id           BIGINT,
    shipping_id         BIGINT,
    shipping_method     STRING,
    carrier_name        STRING,
    delivery_time_days  INT,
    shipping_cost       DECIMAL(8, 2),
    tracking_enabled    BOOLEAN,
    insurance_available BOOLEAN,
    max_weight          DECIMAL(8, 3),
    service_level       STRING,
    large_array         ARRAY<STRING>,
    PRIMARY KEY (shipping_id) NOT ENFORCED
) WITH (
      'connector' = 'upsert-kafka',
      'topic' = 'shipping_topic',
      'properties.bootstrap.servers' = 'localhost:9092',
      'key.format' = 'json',
      'value.format' = 'json'
      );

-- Set up the Payment Kafka sink table with upsert support
CREATE TABLE PaymentKafka
(
    tenant_id               BIGINT,
    payment_id              BIGINT,
    payment_method          STRING,
    payment_provider        STRING,
    transaction_fee         DECIMAL(5, 2),
    processing_time_minutes INT,
    security_level          STRING,
    supported_currencies    STRING,
    max_transaction_amount  DECIMAL(10, 2),
    is_active               BOOLEAN,
    large_array             ARRAY<STRING>,
    PRIMARY KEY (payment_id) NOT ENFORCED
) WITH (
      'connector' = 'upsert-kafka',
      'topic' = 'payment_topic',
      'properties.bootstrap.servers' = 'localhost:9092',
      'key.format' = 'json',
      'value.format' = 'json'
      );

-- Set up the Inventory Kafka sink table with upsert support
CREATE TABLE InventoryKafka
(
    tenant_id         BIGINT,
    inventory_id      BIGINT,
    product_id        BIGINT,
    warehouse_id      BIGINT,
    quantity_on_hand  INT,
    reorder_level     INT,
    reorder_quantity  INT,
    last_restock_date TIMESTAMP(3),
    expiry_date       TIMESTAMP(3),
    storage_location  STRING,
    large_array       ARRAY<STRING>,
    PRIMARY KEY (inventory_id) NOT ENFORCED
) WITH (
      'connector' = 'upsert-kafka',
      'topic' = 'inventory_topic',
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
--       'topic' = 'join_results_mj_topic',
--       'properties.bootstrap.servers' = 'localhost:9092',
--       'key.format' = 'json',
--       'value.format' = 'json'

--         'connector' = 'kafka',
--         'topic' = 'join_results_mj_topic',
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
--       'topic' = 'join_results_binary_topic',
--       'properties.bootstrap.servers' = 'localhost:9092',
--       'key.format' = 'json',
--       'value.format' = 'json'

--       'connector' = 'kafka',
--       'topic' = 'join_results_binary_topic',
--       'properties.bootstrap.servers' = 'localhost:9092',
--       'format' = 'json'
      );

-- 10-way Join: Using subqueries with explicit key preservation

-- Insert tenant data into Kafka
INSERT INTO TenantKafka
SELECT tenant_id,
       tentant_name,
       email,
       registration_date,
       loyalty_level,
       country,
       city,
       tenant_segment,
       large_array
FROM TenantDataGen;

-- Insert supplier data into Kafka
INSERT INTO SuppliersKafka
SELECT tenant_id,
       supplier_id,
       supplier_name,
       contact_person,
       phone,
       address,
       rating,
       active_since,
       supplier_type,
       reliability_score,
       large_array
FROM SupplierDataGen;

-- Insert product data into Kafka
INSERT INTO ProductsKafka
SELECT tenant_id,
       product_id,
       product_name,
       category_id,
       brand,
       supplier_id,
       cost,
       weight,
       product_rating,
       is_active,
       large_array
FROM ProductDataGen;

-- Insert category data into Kafka
INSERT INTO CategoriesKafka
SELECT tenant_id,
       category_id,
       category_name,
       description,
       parent_category,
       is_active,
       created_date,
       category_level,
       display_order,
       large_array
FROM CategoryDataGen;

-- Insert order data into Kafka
INSERT INTO OrdersKafka
SELECT tenant_id,
       order_id,
       product_id,
       supplier_id,
       category_id,
       quantity,
       unit_price,
       order_time,
       region,
       payment_method,
       order_status,
       large_array
FROM OrderDataGen;

-- Insert customer data into Kafka
INSERT INTO CustomersKafka
SELECT tenant_id,
       customer_id,
       customer_name,
       email,
       phone,
       address,
       customer_type,
       registration_date,
       loyalty_points,
       preferred_payment_method,
       large_array
FROM CustomerDataGen;

-- Insert warehouse data into Kafka
INSERT INTO WarehousesKafka
SELECT tenant_id,
       warehouse_id,
       warehouse_name,
       location,
       capacity,
       manager_name,
       contact_phone,
       warehouse_type,
       operational_hours,
       efficiency_rating,
       large_array
FROM WarehouseDataGen;

-- Insert shipping data into Kafka
INSERT INTO ShippingKafka
SELECT tenant_id,
       shipping_id,
       shipping_method,
       carrier_name,
       delivery_time_days,
       shipping_cost,
       tracking_enabled,
       insurance_available,
       max_weight,
       service_level,
       large_array
FROM ShippingDataGen;

-- Insert payment data into Kafka
INSERT INTO PaymentKafka
SELECT tenant_id,
       payment_id,
       payment_method,
       payment_provider,
       transaction_fee,
       processing_time_minutes,
       security_level,
       supported_currencies,
       max_transaction_amount,
       is_active,
       large_array
FROM PaymentDataGen;

-- Insert inventory data into Kafka
INSERT INTO InventoryKafka
SELECT tenant_id,
       inventory_id,
       product_id,
       warehouse_id,
       quantity_on_hand,
       reorder_level,
       reorder_quantity,
       last_restock_date,
       expiry_date,
       storage_location,
       large_array
FROM InventoryDataGen;

-- Set parallelism for 10-way join
SET
'parallelism.default' = '10';
SET
'table.optimizer.multi-join.enabled' = 'true';

INSERT INTO JoinResultsMJ
SELECT t.tenant_id                    as tenant_id,
       o.order_id                     AS order_id,
       COALESCE(s.supplier_id, -1)    AS supplier_id,
       COALESCE(p.product_id, -1)     AS product_id,
       COALESCE(c.category_id, -1)    AS category_id,
       COALESCE(cust.customer_id, -1) AS customer_id,
       COALESCE(w.warehouse_id, -1)   AS warehouse_id,
       COALESCE(sh.shipping_id, -1)   AS shipping_id,
       COALESCE(pay.payment_id, -1)   AS payment_id,
       COALESCE(i.inventory_id, -1)   AS inventory_id
FROM OrdersKafka o
         LEFT JOIN SuppliersKafka s ON o.tenant_id = s.tenant_id
         LEFT JOIN ProductsKafka p ON o.tenant_id = p.tenant_id
         LEFT JOIN CategoriesKafka c ON o.tenant_id = c.tenant_id
         LEFT JOIN TenantKafka t ON o.tenant_id = t.tenant_id
         LEFT JOIN CustomersKafka cust ON o.tenant_id = cust.tenant_id
         LEFT JOIN WarehousesKafka w ON o.tenant_id = w.tenant_id
         LEFT JOIN ShippingKafka sh ON o.tenant_id = sh.tenant_id
         LEFT JOIN PaymentKafka pay ON o.tenant_id = pay.tenant_id
         LEFT JOIN InventoryKafka i ON o.tenant_id = i.tenant_id
         CROSS JOIN (SELECT 1 AS dummy_col) AS d;
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
       COALESCE(i.inventory_id, -1)   AS inventory_id
FROM OrdersKafka o
         LEFT JOIN SuppliersKafka s ON o.tenant_id = s.tenant_id
         LEFT JOIN ProductsKafka p ON o.tenant_id = p.tenant_id
         LEFT JOIN CategoriesKafka c ON o.tenant_id = c.tenant_id
         LEFT JOIN TenantKafka t ON o.tenant_id = t.tenant_id
         LEFT JOIN CustomersKafka cust ON o.tenant_id = cust.tenant_id
         LEFT JOIN WarehousesKafka w ON o.tenant_id = w.tenant_id
         LEFT JOIN ShippingKafka sh ON o.tenant_id = sh.tenant_id
         LEFT JOIN PaymentKafka pay ON o.tenant_id = pay.tenant_id
         LEFT JOIN InventoryKafka i ON o.tenant_id = i.tenant_id
         CROSS JOIN (SELECT 1 AS dummy_col) AS d;
