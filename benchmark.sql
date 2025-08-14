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

-- Set parallelism to 1 for local execution
SET
'parallelism.default' = '20';

-- Set maximum parallelism
SET
'pipeline.max-parallelism' = '20';

SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.timeout' = '5min';
SET 'execution.checkpointing.min-pause' = '2s';
SET 'execution.checkpointing.max-concurrent-checkpoints' = '1';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';

-- Set state backend
SET 'state.backend' = 'rocksdb';
SET 'state.backend.incremental' = 'true';
SET 'state.checkpoints.dir' = 'file:///Users/gdemorais/qdev/flink2/build-target/flink-checkpoints';

SET 'execution.checkpointing.interval' = '30s';
SET 'execution.checkpointing.timeout' = '10min';
SET 'execution.checkpointing.min-pause' = '5s';
SET 'execution.checkpointing.max-concurrent-checkpoints' = '1';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.unaligned' = 'true';
SET 'execution.checkpointing.aligned-checkpoint-timeout' = '1min';


--SET
--'table.optimizer.multi-join.enabled' = 'true';


-- Create normalized tables with tenant_id as the primary key across all tables
-- Tenant dimension table
CREATE TABLE RawTenant (
                              tenant_id BIGINT,
                              tentant_name STRING,
                              email STRING,
                              registration_date TIMESTAMP(3),
                              loyalty_level STRING,
                              country STRING,
                              city STRING,
                              tenant_segment STRING,
                              update_time AS PROCTIME(),
                              PRIMARY KEY (tenant_id) NOT ENFORCED
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

CREATE TABLE Tenant (
                         tenant_id BIGINT,
                         tentant_name STRING,
                         email STRING,
                         registration_date TIMESTAMP(3),
                         loyalty_level STRING,
                         country STRING,
                         city STRING,
                         tenant_segment STRING,
                         update_time AS PROCTIME(),
                         PRIMARY KEY (tenant_id) NOT ENFORCED
) WITH (
      'connector' = 'upsert-files',
      'output-filepath' = 'file:///Users/gdemorais/qdev/flink2/build-target/tenant-data',
      'key.format' = 'json',
      'value.format' = 'json'
      ) as (
SELECT
    tenant_id,
    tentant_name,
    email,
    registration_date,
    loyalty_level,
    country,
    city,
    tenant_segment,
    update_time
FROM (
                  SELECT *, ROW_NUMBER() OVER (PARTITION BY tenant_id ORDER BY update_time DESC)
    AS row_num
                  FROM RawTenant
              ) WHERE row_num = 1 );

-- Supplier dimension table
CREATE TABLE RawSuppliers (
                              tenant_id BIGINT,
                              supplier_id BIGINT,
                              supplier_name STRING,
                              contact_person STRING,
                              phone STRING,
                              address STRING,
                              rating DECIMAL(3,2),
                              active_since TIMESTAMP(3),
                              supplier_type STRING,
                              reliability_score DECIMAL(3,2),
                              update_time AS PROCTIME(),
                              PRIMARY KEY (tenant_id) NOT ENFORCED
) WITH (
      'connector' = 'datagen',
      'rows-per-second' = '10000',
      'fields.tenant_id.min' = '1',
      'fields.tenant_id.max' = '10000',
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
      'fields.reliability_score.max' = '1.0'
      );

CREATE TEMPORARY VIEW Suppliers AS
SELECT * FROM (
                  SELECT *, ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY update_time DESC) AS row_num
                  FROM RawSuppliers
              ) WHERE row_num = 1;

-- Product dimension table
CREATE TABLE RawProducts (
                             tenant_id BIGINT,
                             product_id BIGINT,
                             product_name STRING,
                             category_id BIGINT,
                             brand STRING,
                             supplier_id BIGINT,
                             cost DECIMAL(10,2),
                             weight DECIMAL(8,3),
                             product_rating DECIMAL(3,2),
                             is_active BOOLEAN,
                             update_time AS PROCTIME(),
                             PRIMARY KEY (tenant_id) NOT ENFORCED
) WITH (
      'connector' = 'datagen',
      'rows-per-second' = '10000',
      'fields.tenant_id.min' = '1',
      'fields.tenant_id.max' = '10000',
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
      'fields.product_rating.max' = '5.0'
      );

CREATE TEMPORARY VIEW Products AS
SELECT * FROM (
                  SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY update_time DESC) AS row_num
                  FROM RawProducts
              ) WHERE row_num = 1;

-- Category dimension table
CREATE TABLE RawCategories (
                               tenant_id BIGINT,
                               category_id BIGINT,
                               category_name STRING,
                               description STRING,
                               parent_category STRING,
                               is_active BOOLEAN,
                               created_date TIMESTAMP(3),
                               category_level INT,
                               display_order INT,
                               update_time AS PROCTIME(),
                               PRIMARY KEY (tenant_id) NOT ENFORCED
) WITH (
      'connector' = 'datagen',
      'rows-per-second' = '10000',
      'fields.tenant_id.min' = '1',
      'fields.tenant_id.max' = '10000',
      'fields.category_id.min' = '1',
      'fields.category_id.max' = '100000000',
      'fields.category_name.length' = '12',
      'fields.description.length' = '30',
      'fields.parent_category.length' = '2',
      'fields.category_level.min' = '1',
      'fields.category_level.max' = '3',
      'fields.display_order.min' = '1',
      'fields.display_order.max' = '100'
      );

CREATE TEMPORARY VIEW Categories AS
SELECT * FROM (
                  SELECT *, ROW_NUMBER() OVER (PARTITION BY category_id ORDER BY update_time DESC) AS row_num
                  FROM RawCategories
              ) WHERE row_num = 1;

-- Main Orders table (fact table)
CREATE TABLE RawOrders (
                           tenant_id BIGINT,
                           order_id BIGINT,
                           product_id BIGINT,
                           supplier_id BIGINT,
                           category_id BIGINT,
                           quantity INT,
                           unit_price DECIMAL(10,2),
                           order_time TIMESTAMP(3),
                           region STRING,
                           payment_method STRING,
                           order_status STRING,
                           update_time AS PROCTIME(),
                           PRIMARY KEY (tenant_id) NOT ENFORCED
) WITH (
      'connector' = 'datagen',
      'rows-per-second' = '1000',
      'fields.tenant_id.min' = '1',
      'fields.tenant_id.max' = '10000',
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
      'fields.order_status.length' = '1'
      );

CREATE TEMPORARY VIEW Orders AS
SELECT * FROM (
                  SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY update_time DESC) AS row_num
                  FROM RawOrders
              ) WHERE row_num = 1;

-- Create sink tables for results
CREATE TABLE JoinResults (
                             tenant_id BIGINT,
                             order_id BIGINT,
                             tentant_name STRING,
                             product_name STRING,
                             supplier_name STRING,
                             category_name STRING,
                             total_amount DECIMAL(12,2),
                             profit_margin DECIMAL(10,2),
                             order_time TIMESTAMP(3),
                             region STRING,
                             tenant_segment STRING,
                             supplier_type STRING
) WITH (
      'connector' = 'blackhole'
      );

-- Binary Join Approach: Multiple sequential LEFT JOINs
-- Joins based only on tenant_id
INSERT INTO JoinResults
SELECT
    o.tenant_id,
    o.order_id,
    c.tentant_name,
    p.product_name,
    s.supplier_name,
    cat.category_name,
    o.quantity * o.unit_price as total_amount,
    (o.quantity * o.unit_price) - (o.quantity * p.cost) as profit_margin,
    o.order_time,
    o.region,
    c.tenant_segment,
    s.supplier_type
FROM Tenant c
         LEFT JOIN Suppliers s ON c.tenant_id = s.tenant_id
         LEFT JOIN Products p ON c.tenant_id = p.tenant_id
         LEFT JOIN Categories cat ON c.tenant_id = cat.tenant_id
         LEFT JOIN Orders o ON c.tenant_id = o.tenant_id;
