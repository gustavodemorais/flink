-- Flink Upsert-Kafka Connector Local Example with Faker

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
SET
'execution.checkpointing.aligned-checkpoint-timeout' = '1min';

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

-- Set up the faker source table to generate synthetic tenant data
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
    large_array       STRING,

    PRIMARY KEY (tenant_id) NOT ENFORCED
) WITH (
      'connector' = 'faker',
      'number-of-rows' = '100',
      'rows-per-second' = '10',
      'changelog.mode' = 'upsert',
      'fields.tenant_id.expression' = '#{number.numberBetween ''1'',''1000''}',
      'fields.tentant_name.expression' = '#{Company.name}',
      'fields.email.expression' = '#{Internet.emailAddress}',
      'fields.registration_date.expression' = '#{date.past ''1000'',''DAYS''}',
      'fields.loyalty_level.expression' = '#{options.option ''A'',''B'',''C''}',
      'fields.country.expression' = '#{Address.countryCode}',
      'fields.city.expression' = '#{Address.city}',
      'fields.tenant_segment.expression' = '#{options.option ''A'',''B'',''C''}',
      'fields.large_array.expression' = '#{Lorem.words ''1000''}'
      );

-- Supplier dimension table with faker source
CREATE TABLE SupplierDataGen
(
    tenant_id         BIGINT,
    supplier_id       BIGINT,
    supplier_name     STRING,
    contact_person    STRING,
    phone             STRING,
    address           STRING,
    rating            FLOAT,
    active_since      TIMESTAMP(3),
    supplier_type     STRING,
    reliability_score FLOAT,
    large_array       STRING,

    PRIMARY KEY (supplier_id) NOT ENFORCED
) WITH (
      'connector' = 'faker',
      'number-of-rows' = '100',
      'rows-per-second' = '10',
      'changelog.mode' = 'upsert',
      'fields.tenant_id.expression' = '#{number.numberBetween ''1'',''1000''}',
      'fields.supplier_id.expression' = '#{number.numberBetween ''1'',''100000000''}',
      'fields.supplier_name.expression' = '#{Company.name}',
      'fields.contact_person.expression' = '#{Name.fullName}',
      'fields.phone.expression' = '#{numerify ''##########''}',
      'fields.address.expression' = '#{Address.fullAddress}',
      'fields.rating.expression' = '#{number.randomDouble ''2'',''1'',''5''}',
      'fields.active_since.expression' = '#{date.past ''3650'',''DAYS''}',
      'fields.supplier_type.expression' = '#{options.option ''A'',''B'',''C''}',
      'fields.reliability_score.expression' = '#{number.randomDouble ''2'',''0'',''1''}',
      'fields.large_array.expression' = '#{Lorem.words ''1''}'
      );

-- Product dimension table with faker source
CREATE TABLE ProductDataGen
(
    tenant_id      BIGINT,
    product_id     BIGINT,
    product_name   STRING,
    category_id    BIGINT,
    brand          STRING,
    supplier_id    BIGINT,
    cost           FLOAT,
    weight         FLOAT,
    product_rating FLOAT,
    is_active      BOOLEAN,
    large_array    STRING,

    PRIMARY KEY (product_id) NOT ENFORCED
) WITH (
      'connector' = 'faker',
      'number-of-rows' = '100',
      'rows-per-second' = '10',
      'changelog.mode' = 'upsert',
      'fields.tenant_id.expression' = '#{number.numberBetween ''1'',''1000''}',
      'fields.product_id.expression' = '#{number.numberBetween ''1'',''100000000''}',
      'fields.product_name.expression' = '#{Commerce.productName}',
      'fields.category_id.expression' = '#{number.numberBetween ''1'',''1000''}',
      'fields.brand.expression' = '#{Company.name}',
      'fields.supplier_id.expression' = '#{number.numberBetween ''1'',''500''}',
      'fields.cost.expression' = '#{number.randomDouble ''2'',''5'',''500''}',
      'fields.weight.expression' = '#{number.randomDouble ''3'',''1'',''10''}',
      'fields.product_rating.expression' = '#{number.randomDouble ''2'',''1'',''5''}',
      'fields.is_active.expression' = '#{bool.bool}',
      'fields.large_array.expression' = '#{Lorem.words ''1''}'
      );

-- Category dimension table with faker source
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
    large_array     STRING,

    PRIMARY KEY (category_id) NOT ENFORCED
) WITH (
      'connector' = 'faker',
      'number-of-rows' = '100',
      'rows-per-second' = '10',
      'changelog.mode' = 'upsert',
      'fields.tenant_id.expression' = '#{number.numberBetween ''1'',''1000''}',
      'fields.category_id.expression' = '#{number.numberBetween ''1'',''100000000''}',
      'fields.category_name.expression' = '#{Commerce.department}',
      'fields.description.expression' = '#{Lorem.sentence}',
      'fields.parent_category.expression' = '#{options.option ''A'',''B'',''C''}',
      'fields.is_active.expression' = '#{bool.bool}',
      'fields.created_date.expression' = '#{date.past ''2000'',''DAYS''}',
      'fields.category_level.expression' = '#{number.numberBetween ''1'',''3''}',
      'fields.display_order.expression' = '#{number.numberBetween ''1'',''100''}',
      'fields.large_array.expression' = '#{Lorem.words ''1''}'
      );

-- Orders table (fact table) with faker source
CREATE TABLE OrderDataGen
(
    tenant_id      BIGINT,
    order_id       BIGINT,
    product_id     BIGINT,
    supplier_id    BIGINT,
    category_id    BIGINT,
    quantity       INT,
    unit_price     FLOAT,
    order_time     TIMESTAMP(3),
    region         STRING,
    payment_method STRING,
    order_status   STRING,
    large_array    STRING,

    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
      'connector' = 'faker',
      'number-of-rows' = '100',
      'rows-per-second' = '10',
      'changelog.mode' = 'upsert',
      'fields.tenant_id.expression' = '#{number.numberBetween ''1'',''1000''}',
      'fields.order_id.expression' = '#{number.numberBetween ''1'',''100000''}',
      'fields.product_id.expression' = '#{number.numberBetween ''1'',''5000''}',
      'fields.supplier_id.expression' = '#{number.numberBetween ''1'',''500''}',
      'fields.category_id.expression' = '#{number.numberBetween ''1'',''1000''}',
      'fields.quantity.expression' = '#{number.numberBetween ''1'',''10''}',
      'fields.unit_price.expression' = '#{number.randomDouble ''2'',''10'',''1000''}',
      'fields.order_time.expression' = '#{date.past ''365'',''DAYS''}',
      'fields.region.expression' = '#{Address.countryCode}',
      'fields.payment_method.expression' = '#{options.option ''CC'',''PP'',''BT''}',
      'fields.order_status.expression' = '#{options.option ''N'',''P'',''S''}',
      'fields.large_array.expression' = '#{Lorem.words ''1''}'
      );

-- Customer dimension table with faker source
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
    large_array              STRING,

    PRIMARY KEY (customer_id) NOT ENFORCED
) WITH (
      'connector' = 'faker',
      'number-of-rows' = '100',
      'rows-per-second' = '10',
      'changelog.mode' = 'upsert',
      'fields.tenant_id.expression' = '#{number.numberBetween ''1'',''1000''}',
      'fields.customer_id.expression' = '#{number.numberBetween ''1'',''100000000''}',
      'fields.customer_name.expression' = '#{Name.fullName}',
      'fields.email.expression' = '#{Internet.emailAddress}',
      'fields.phone.expression' = '#{numerify ''##########''}',
      'fields.address.expression' = '#{Address.fullAddress}',
      'fields.customer_type.expression' = '#{options.option ''A'',''B'',''C''}',
      'fields.registration_date.expression' = '#{date.past ''2000'',''DAYS''}',
      'fields.loyalty_points.expression' = '#{number.numberBetween ''0'',''10000''}',
      'fields.preferred_payment_method.expression' = '#{options.option ''CC'',''PP'',''BT''}',
      'fields.large_array.expression' = '#{Lorem.words ''1''}'
      );

-- Warehouse dimension table with faker source
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
    efficiency_rating FLOAT,
    large_array       STRING,

    PRIMARY KEY (warehouse_id) NOT ENFORCED
) WITH (
      'connector' = 'faker',
      'number-of-rows' = '100',
      'rows-per-second' = '10',
      'changelog.mode' = 'upsert',
      'fields.tenant_id.expression' = '#{number.numberBetween ''1'',''1000''}',
      'fields.warehouse_id.expression' = '#{number.numberBetween ''1'',''100000000''}',
      'fields.warehouse_name.expression' = '#{Company.name}',
      'fields.location.expression' = '#{Address.city}',
      'fields.capacity.expression' = '#{number.numberBetween ''1000'',''50000''}',
      'fields.manager_name.expression' = '#{Name.fullName}',
      'fields.contact_phone.expression' = '#{numerify ''##########''}',
      'fields.warehouse_type.expression' = '#{options.option ''A'',''B'',''C''}',
      'fields.operational_hours.expression' = '#{options.option ''DAY'',''NIGHT''}',
      'fields.efficiency_rating.expression' = '#{number.randomDouble ''2'',''0'',''1''}',
      'fields.large_array.expression' = '#{Lorem.words ''1''}'
      );

-- Shipping dimension table with faker source
CREATE TABLE ShippingDataGen
(
    tenant_id           BIGINT,
    shipping_id         BIGINT,
    shipping_method     STRING,
    carrier_name        STRING,
    delivery_time_days  INT,
    shipping_cost       FLOAT,
    tracking_enabled    BOOLEAN,
    insurance_available BOOLEAN,
    max_weight          FLOAT,
    service_level       STRING,
    large_array         STRING,

    PRIMARY KEY (shipping_id) NOT ENFORCED
) WITH (
      'connector' = 'faker',
      'number-of-rows' = '100',
      'rows-per-second' = '10',
      'changelog.mode' = 'upsert',
      'fields.tenant_id.expression' = '#{number.numberBetween ''1'',''1000''}',
      'fields.shipping_id.expression' = '#{number.numberBetween ''1'',''100000000''}',
      'fields.shipping_method.expression' = '#{options.option ''STANDARD'',''EXPRESS''}',
      'fields.carrier_name.expression' = '#{Company.name}',
      'fields.delivery_time_days.expression' = '#{number.numberBetween ''1'',''30''}',
      'fields.shipping_cost.expression' = '#{number.randomDouble ''2'',''5'',''100''}',
      'fields.tracking_enabled.expression' = '#{bool.bool}',
      'fields.insurance_available.expression' = '#{bool.bool}',
      'fields.max_weight.expression' = '#{number.randomDouble ''3'',''1'',''50''}',
      'fields.service_level.expression' = '#{options.option ''A'',''B'',''C''}',
      'fields.large_array.expression' = '#{Lorem.words ''1''}'
      );

-- Payment dimension table with faker source
CREATE TABLE PaymentDataGen
(
    tenant_id               BIGINT,
    payment_id              BIGINT,
    payment_method          STRING,
    payment_provider        STRING,
    transaction_fee         FLOAT,
    processing_time_minutes INT,
    security_level          STRING,
    supported_currencies    STRING,
    max_transaction_amount  FLOAT,
    is_active               BOOLEAN,
    large_array             STRING,

    PRIMARY KEY (payment_id) NOT ENFORCED
) WITH (
      'connector' = 'faker',
      'number-of-rows' = '100',
      'rows-per-second' = '10',
      'changelog.mode' = 'upsert',
      'fields.tenant_id.expression' = '#{number.numberBetween ''1'',''1000''}',
      'fields.payment_id.expression' = '#{number.numberBetween ''1'',''100000000''}',
      'fields.payment_method.expression' = '#{options.option ''CARD'',''PAYPAL'',''WIRE''}',
      'fields.payment_provider.expression' = '#{Company.name}',
      'fields.transaction_fee.expression' = '#{number.randomDouble ''2'',''0'',''5''}',
      'fields.processing_time_minutes.expression' = '#{number.numberBetween ''1'',''60''}',
      'fields.security_level.expression' = '#{options.option ''L'',''M'',''H''}',
      'fields.supported_currencies.expression' = '#{options.option ''USD,EUR'',''USD,GBP'',''EUR,GBP''}',
      'fields.max_transaction_amount.expression' = '#{number.randomDouble ''2'',''100'',''10000''}',
      'fields.is_active.expression' = '#{bool.bool}',
      'fields.large_array.expression' = '#{Lorem.words ''1''}'
      );

-- Inventory dimension table with faker source
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
    large_array       STRING,

    PRIMARY KEY (inventory_id) NOT ENFORCED
) WITH (
      'connector' = 'faker',
      'number-of-rows' = '100',
      'rows-per-second' = '10',
      'changelog.mode' = 'upsert',
      'fields.tenant_id.expression' = '#{number.numberBetween ''1'',''1000''}',
      'fields.inventory_id.expression' = '#{number.numberBetween ''1'',''100000000''}',
      'fields.product_id.expression' = '#{number.numberBetween ''1'',''5000''}',
      'fields.warehouse_id.expression' = '#{number.numberBetween ''1'',''100''}',
      'fields.quantity_on_hand.expression' = '#{number.numberBetween ''0'',''1000''}',
      'fields.reorder_level.expression' = '#{number.numberBetween ''10'',''100''}',
      'fields.reorder_quantity.expression' = '#{number.numberBetween ''50'',''500''}',
      'fields.last_restock_date.expression' = '#{date.past ''365'',''DAYS''}',
      'fields.expiry_date.expression' = '#{date.future ''365'',''DAYS''}',
      'fields.storage_location.expression' = '#{regexify ''[A-Z]{1}[0-9]{1,2}''}',
      'fields.large_array.expression' = '#{Lorem.words ''1''}'
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
    rating            FLOAT,
    active_since      TIMESTAMP(3),
    supplier_type     STRING,
    reliability_score FLOAT,
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
    cost           FLOAT,
    weight         FLOAT,
    product_rating FLOAT,
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
    unit_price     FLOAT,
    order_time     TIMESTAMP(3),
    region         STRING,
    payment_method STRING,
    order_status   STRING,
    large_array    ARRAY<STRING>,

    PRIMARY KEY (order_id) NOT ENFORCED
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
    efficiency_rating FLOAT,
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
    shipping_cost       FLOAT,
    tracking_enabled    BOOLEAN,
    insurance_available BOOLEAN,
    max_weight          FLOAT,
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
    transaction_fee         FLOAT,
    processing_time_minutes INT,
    security_level          STRING,
    supported_currencies    STRING,
    max_transaction_amount  FLOAT,
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
    supplier_id  BIGINT,
    product_id   BIGINT,
    category_id  BIGINT,
    order_id     BIGINT,
    customer_id  BIGINT,
    warehouse_id BIGINT,
    shipping_id  BIGINT,
    payment_id   BIGINT,
    inventory_id BIGINT
) WITH (
      'connector' = 'kafka',
      'topic' = 'join_results_mj_topic',
      'properties.bootstrap.servers' = 'localhost:9092',
      'format' = 'json'
      );

-- Set up the Join Results Kafka sink table
CREATE TABLE JoinResultsBinary
(
    tenant_id    BIGINT,
    supplier_id  BIGINT,
    product_id   BIGINT,
    category_id  BIGINT,
    order_id     BIGINT,
    customer_id  BIGINT,
    warehouse_id BIGINT,
    shipping_id  BIGINT,
    payment_id   BIGINT,
    inventory_id BIGINT
) WITH (
      'connector' = 'kafka',
      'topic' = 'join_results_binary_topic',
      'properties.bootstrap.servers' = 'localhost:9092',
      'format' = 'json'
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
       COALESCE(s.supplier_id, -1)    AS supplier_id,
       COALESCE(p.product_id, -1)     AS product_id,
       COALESCE(c.category_id, -1)    AS category_id,
       COALESCE(o.order_id, -1)       AS order_id,
       COALESCE(cust.customer_id, -1) AS customer_id,
       COALESCE(w.warehouse_id, -1)   AS warehouse_id,
       COALESCE(sh.shipping_id, -1)   AS shipping_id,
       COALESCE(pay.payment_id, -1)   AS payment_id,
       COALESCE(i.inventory_id, -1)   AS inventory_id
FROM TenantKafka t
         LEFT JOIN SuppliersKafka s ON t.tenant_id = s.tenant_id
         LEFT JOIN ProductsKafka p ON t.tenant_id = p.tenant_id
         LEFT JOIN CategoriesKafka c ON t.tenant_id = c.tenant_id
         LEFT JOIN OrdersKafka o ON t.tenant_id = o.tenant_id
         LEFT JOIN CustomersKafka cust ON t.tenant_id = cust.tenant_id
         LEFT JOIN WarehousesKafka w ON t.tenant_id = w.tenant_id
         LEFT JOIN ShippingKafka sh ON t.tenant_id = sh.tenant_id
         LEFT JOIN PaymentKafka pay ON t.tenant_id = pay.tenant_id
         LEFT JOIN InventoryKafka i ON t.tenant_id = i.tenant_id;
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
       COALESCE(s.supplier_id, -1)    AS supplier_id,
       COALESCE(p.product_id, -1)     AS product_id,
       COALESCE(c.category_id, -1)    AS category_id,
       COALESCE(o.order_id, -1)       AS order_id,
       COALESCE(cust.customer_id, -1) AS customer_id,
       COALESCE(w.warehouse_id, -1)   AS warehouse_id,
       COALESCE(sh.shipping_id, -1)   AS shipping_id,
       COALESCE(pay.payment_id, -1)   AS payment_id,
       COALESCE(i.inventory_id, -1)   AS inventory_id
FROM TenantKafka t
         LEFT JOIN SuppliersKafka s ON t.tenant_id = s.tenant_id
         LEFT JOIN ProductsKafka p ON t.tenant_id = p.tenant_id
         LEFT JOIN CategoriesKafka c ON t.tenant_id = c.tenant_id
         LEFT JOIN OrdersKafka o ON t.tenant_id = o.tenant_id
         LEFT JOIN CustomersKafka cust ON t.tenant_id = cust.tenant_id
         LEFT JOIN WarehousesKafka w ON t.tenant_id = w.tenant_id
         LEFT JOIN ShippingKafka sh ON t.tenant_id = sh.tenant_id
         LEFT JOIN PaymentKafka pay ON t.tenant_id = pay.tenant_id
         LEFT JOIN InventoryKafka i ON t.tenant_id = i.tenant_id;
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


/*
Example queries to test the join results

SELECT *
FROM `SupplierDataGen` s
LEFT JOIN `ProductDataGen` p ON s.tenant_id = p.tenant_id
LEFT JOIN `CategoryDataGen` c ON s.tenant_id = c.tenant_id
LEFT JOIN `OrderDataGen` o ON s.tenant_id = o.tenant_id
LEFT JOIN `CustomerDataGen` cust ON s.tenant_id = cust.tenant_id;

SELECT *
FROM `SupplierDataGen` s
LEFT JOIN `ProductDataGen` p ON s.tenant_id = p.tenant_id
LEFT JOIN `CategoryDataGen` c ON s.tenant_id = c.tenant_id
LEFT JOIN `OrderDataGen` o ON s.tenant_id = o.tenant_id
LEFT JOIN `CustomerDataGen` cust ON s.tenant_id = cust.tenant_id
LEFT JOIN `WarehouseDataGen` w ON s.tenant_id = w.tenant_id
LEFT JOIN `ShippingDataGen` sh ON s.tenant_id = sh.tenant_id
LEFT JOIN `PaymentDataGen` pay ON s.tenant_id = pay.tenant_id
LEFT JOIN `InventoryDataGen` i ON s.tenant_id = i.tenant_id
LEFT JOIN `TenantDataGen` t ON s.tenant_id = t.tenant_id;


SELECT *
FROM `SupplierDataGen` s
LEFT JOIN `ProductDataGen` p
  ON s.tenant_id = p.tenant_id
 AND s.supplier_id = p.supplier_id
LEFT JOIN `CategoryDataGen` c
  ON p.tenant_id = c.tenant_id
 AND p.category_id = c.category_id
LEFT JOIN `OrderDataGen` o
  ON o.tenant_id = p.tenant_id
 AND o.product_id = p.product_id
 AND o.supplier_id = s.supplier_id
 AND o.category_id = c.category_id
LEFT JOIN `CustomerDataGen` cust
  ON cust.tenant_id = s.tenant_id
WHERE
  p.product_id IS NOT NULL
  AND c.category_id IS NOT NULL
  AND o.order_id IS NOT NULL;

SELECT *
FROM `SupplierDataGen` s
LEFT JOIN `ProductDataGen` p
  ON s.tenant_id = p.tenant_id
 AND s.supplier_id = p.supplier_id
LEFT JOIN `CategoryDataGen` c
  ON p.tenant_id = c.tenant_id
 AND p.category_id = c.category_id
LEFT JOIN `OrderDataGen` o
  ON o.tenant_id = p.tenant_id
 AND o.product_id = p.product_id
 AND o.supplier_id = s.supplier_id
 AND o.category_id = c.category_id
LEFT JOIN `InventoryDataGen` i
  ON i.tenant_id = p.tenant_id
 AND i.product_id = p.product_id
LEFT JOIN `WarehouseDataGen` w
  ON w.tenant_id = i.tenant_id
 AND w.warehouse_id = i.warehouse_id
LEFT JOIN `ShippingDataGen` sh
  ON sh.tenant_id = s.tenant_id
LEFT JOIN `PaymentDataGen` pay
  ON pay.tenant_id = s.tenant_id
LEFT JOIN `CustomerDataGen` cust
  ON cust.tenant_id = s.tenant_id
LEFT JOIN `TenantDataGen` t
  ON t.tenant_id = s.tenant_id
WHERE
  p.product_id IS NOT NULL
  AND c.category_id IS NOT NULL
  AND o.order_id IS NOT NULL
  AND i.inventory_id IS NOT NULL
  AND w.warehouse_id IS NOT NULL;

*/
