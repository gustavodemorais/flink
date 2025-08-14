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
-- SET 'execution.target' = 'local';

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

SET
'table.optimizer.multi-join.enabled' = 'true';

SET 'execution.checkpointing.interval' = '30s';
SET 'execution.checkpointing.timeout' = '10min';
SET 'execution.checkpointing.min-pause' = '5s';
SET 'execution.checkpointing.max-concurrent-checkpoints' = '1';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.unaligned' = 'true';
SET 'execution.checkpointing.aligned-checkpoint-timeout' = '1min';

-- Resource configuration for limited environments


-- Create normalized tables with order_id as the primary key across all tables
-- Main Orders table (fact table)
CREATE TABLE Orders (
                        order_id BIGINT,
                        customer_id BIGINT,
                        product_id BIGINT,
                        supplier_id BIGINT,
                        category_id STRING,
                        quantity INT,
                        unit_price DECIMAL(10,2),
                        order_time TIMESTAMP(3),
                        region STRING,
                        payment_method STRING,
                        order_status STRING,
                        PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
      'connector' = 'datagen',
      'rows-per-second' = '10000',
      /*'fields.order_id.kind' = 'sequence',
      'fields.order_id.start' = '1',
      'fields.order_id.end' = '200',*/
      'fields.order_id.min' = '1',
      'fields.order_id.max' = '100000000',
      'fields.customer_id.min' = '1',
      'fields.customer_id.max' = '1000',
      'fields.product_id.min' = '1',
      'fields.product_id.max' = '5000',
      'fields.supplier_id.min' = '1',
      'fields.supplier_id.max' = '500',
      'fields.category_id.length' = '2',
      'fields.quantity.min' = '1',
      'fields.quantity.max' = '10',
      'fields.unit_price.min' = '10.00',
      'fields.unit_price.max' = '1000.00',
      'fields.region.length' = '3',
      'fields.payment_method.length' = '2',
      'fields.order_status.length' = '1'
      );

-- Customer dimension table
CREATE TABLE Customers (
                           order_id BIGINT,
                           customer_id BIGINT,
                           customer_name STRING,
                           email STRING,
                           registration_date TIMESTAMP(3),
                           loyalty_level STRING,
                           country STRING,
                           city STRING,
                           customer_segment STRING,
                           PRIMARY KEY (customer_id) NOT ENFORCED
) WITH (
      'connector' = 'datagen',
      'rows-per-second' = '80',
      'fields.order_id.min' = '1',
      'fields.order_id.max' = '100000000',
      'fields.customer_id.min' = '1',
      'fields.customer_id.max' = '1000',  -- Limited to 1000
      'fields.customer_name.length' = '15',
      'fields.email.length' = '20',
      'fields.loyalty_level.length' = '1',
      'fields.country.length' = '3',
      'fields.city.length' = '10',
      'fields.customer_segment.length' = '1'
      );

-- Product dimension table
CREATE TABLE Products (
                          order_id BIGINT,
                          product_id BIGINT,
                          product_name STRING,
                          category_id STRING,
                          brand STRING,
                          supplier_id BIGINT,
                          cost DECIMAL(10,2),
                          weight DECIMAL(8,3),
                          product_rating DECIMAL(3,2),
                          is_active BOOLEAN,
                          PRIMARY KEY (product_id) NOT ENFORCED
) WITH (
      'connector' = 'datagen',
      'rows-per-second' = '60',
      'fields.order_id.min' = '1',
      'fields.order_id.max' = '100000000',
      'fields.product_id.min' = '1',
      'fields.product_id.max' = '5000',
      'fields.product_name.length' = '20',
      'fields.category_id.length' = '2',
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

-- Supplier dimension table
CREATE TABLE Suppliers (
                           order_id BIGINT,
                           supplier_id BIGINT,
                           supplier_name STRING,
                           contact_person STRING,
                           phone STRING,
                           address STRING,
                           rating DECIMAL(3,2),
                           active_since TIMESTAMP(3),
                           supplier_type STRING,
                           reliability_score DECIMAL(3,2),
                           PRIMARY KEY (supplier_id) NOT ENFORCED
) WITH (
      'connector' = 'datagen',
      'rows-per-second' = '40',
      'fields.order_id.min' = '1',
      'fields.order_id.max' = '100000000',
      'fields.supplier_id.min' = '1',
      'fields.supplier_id.max' = '500',
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

-- Category dimension table
CREATE TABLE Categories (
                            order_id BIGINT,
                            category_id STRING,
                            category_name STRING,
                            description STRING,
                            parent_category STRING,
                            is_active BOOLEAN,
                            created_date TIMESTAMP(3),
                            category_level INT,
                            display_order INT,
                            PRIMARY KEY (category_id) NOT ENFORCED
) WITH (
      'connector' = 'datagen',
      'rows-per-second' = '100',
      'fields.order_id.min' = '1',
      'fields.order_id.max' = '100000000',
      'fields.category_id.length' = '2',
      'fields.category_name.length' = '12',
      'fields.description.length' = '30',
      'fields.parent_category.length' = '2',
      'fields.category_level.min' = '1',
      'fields.category_level.max' = '3',
      'fields.display_order.min' = '1',
      'fields.display_order.max' = '100'
      );

-- Create sink tables for results
CREATE TABLE JoinResults (
                             order_id BIGINT,
                             customer_name STRING,
                             product_name STRING,
                             supplier_name STRING,
                             category_name STRING,
                             total_amount DECIMAL(12,2),
                             profit_margin DECIMAL(10,2),
                             order_time TIMESTAMP(3),
                             region STRING,
                             customer_segment STRING,
                             supplier_type STRING,
                             dummy_column STRING
) WITH (
      'connector' = 'blackhole'
      );

CREATE TABLE JoinResultsCount (
                                  result_count BIGINT
) WITH (
      'connector' = 'print'
      );

-- Binary Join Approach: Multiple sequential LEFT JOINs
-- Joins based only on order_id
INSERT INTO JoinResults
SELECT
    o.order_id,
    c.customer_name,
    p.product_name,
    s.supplier_name,
    cat.category_name,
    o.quantity * o.unit_price as total_amount,
    (o.quantity * o.unit_price) - (o.quantity * p.cost) as profit_margin,
    o.order_time,
    o.region,
    c.customer_segment,
    s.supplier_type,
    'dummy' as dummy_column
FROM Orders o
         LEFT JOIN Customers c ON o.order_id = c.order_id
         LEFT JOIN Products p ON o.order_id = p.order_id
         LEFT JOIN Suppliers s ON o.order_id = s.order_id
         LEFT JOIN Categories cat ON o.order_id = cat.order_id;

-- INSERT INTO JoinResults
--SELECT
--    o.order_id,
--  c.customer_name,
--  p.product_name,
--  s.supplier_name,
--  cat.category_name,
--  o.quantity * o.unit_price as total_amount,
--  (o.quantity * o.unit_price) - (o.quantity * p.cost) as profit_margin,
--  o.order_time,
--  o.region,
--  c.customer_segment,
--  s.supplier_type,
--  d.dummy_value as dummy_column
--FROM Orders o
--         LEFT JOIN Customers c ON o.order_id = c.order_id
--         LEFT JOIN Products p ON o.order_id = p.order_id
--       LEFT JOIN Suppliers s ON o.order_id = s.order_id
--       LEFT JOIN Categories cat ON o.order_id = cat.order_id
--       RIGHT JOIN (VALUES ('DUMMY_VALUE')) AS d(dummy_value) ON TRUE;
