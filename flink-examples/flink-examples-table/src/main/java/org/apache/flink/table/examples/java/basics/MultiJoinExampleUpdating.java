/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.examples.java.basics;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Multi-Join Example with Updating Streams demonstrating advanced streaming join patterns in Flink
 * SQL.
 *
 * <p>This example shows: 1. Setting up streaming execution with RocksDB state backend and
 * checkpointing 2. Creating raw tables with datagen connector for continuous data generation 3.
 * Creating deduplication views using ROW_NUMBER() to produce update streams 4. Performing multi-way
 * joins with composite join conditions (multiple keys) 5. Demonstrating how join key uniqueness
 * affects state management 6. Benchmarking binary vs multi-join optimization
 *
 * <p>Key Features: - Uses deduplication views to convert insert-only streams to update streams -
 * Composite join conditions (e.g., order_id AND customer_id) - Proper state backend configuration
 * for production-like scenarios - Demonstrates key uniqueness loss through join chains
 */
public final class MultiJoinExampleUpdating {

    public static void main(String[] args) throws Exception {

        // Set up the Table API with streaming mode
        final EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inStreamingMode().build();
        final TableEnvironment tableEnv = TableEnvironment.create(settings);

        // Configure execution settings for optimal performance and stability
        tableEnv.getConfig().set("parallelism.default", "4");
        tableEnv.getConfig().set("pipeline.max-parallelism", "4");
        tableEnv.getConfig().set("execution.checkpointing.interval", "30s");
        tableEnv.getConfig().set("execution.checkpointing.timeout", "10min");
        tableEnv.getConfig().set("execution.checkpointing.min-pause", "5s");
        tableEnv.getConfig().set("execution.checkpointing.max-concurrent-checkpoints", "1");
        tableEnv.getConfig().set("execution.checkpointing.mode", "EXACTLY_ONCE");
        tableEnv.getConfig().set("execution.checkpointing.unaligned", "true");
        tableEnv.getConfig().set("execution.checkpointing.aligned-checkpoint-timeout", "1min");

        // State backend configuration
        tableEnv.getConfig().set("state.backend", "rocksdb");
        tableEnv.getConfig().set("state.backend.incremental", "true");
        tableEnv.getConfig().set("state.checkpoints.dir", "file:///tmp/flink-checkpoints");

        // Enable multi-join optimization
        tableEnv.getConfig().set("table.optimizer.multi-join.enabled", "true");

        // Enable flamegraph for performance analysis
        tableEnv.getConfig().set("rest.flamegraph.enabled", "true");

        System.out.println("=== Multi-Join Example with Updating Streams ===");
        System.out.println("Setting up streaming join benchmark with deduplication views...\n");

        // Create RawOrders table (fact table) with data generation
        tableEnv.executeSql(
                "CREATE TABLE RawOrders ("
                        + "    order_id BIGINT,"
                        + "    customer_id BIGINT,"
                        + "    product_id BIGINT,"
                        + "    supplier_id BIGINT,"
                        + "    category_id BIGINT,"
                        + "    quantity INT,"
                        + "    unit_price DECIMAL(10,2),"
                        + "    order_time TIMESTAMP(3),"
                        + "    region STRING,"
                        + "    payment_method STRING,"
                        + "    order_status STRING,"
                        + "    update_time AS PROCTIME()"
                        + ") WITH ("
                        + "    'connector' = 'datagen',"
                        + "    'rows-per-second' = '1000',"
                        + "    'fields.order_id.min' = '1',"
                        + "    'fields.order_id.max' = '100000',"
                        + "    'fields.customer_id.min' = '1',"
                        + "    'fields.customer_id.max' = '1000',"
                        + "    'fields.product_id.min' = '1',"
                        + "    'fields.product_id.max' = '5000',"
                        + "    'fields.supplier_id.min' = '1',"
                        + "    'fields.supplier_id.max' = '500',"
                        + "    'fields.category_id.min' = '1',"
                        + "    'fields.category_id.max' = '1000',"
                        + "    'fields.quantity.min' = '1',"
                        + "    'fields.quantity.max' = '10',"
                        + "    'fields.unit_price.min' = '10.00',"
                        + "    'fields.unit_price.max' = '1000.00',"
                        + "    'fields.region.length' = '3',"
                        + "    'fields.payment_method.length' = '2',"
                        + "    'fields.order_status.length' = '1'"
                        + ")");

        // Create deduplication view for Orders
        tableEnv.executeSql(
                "CREATE TEMPORARY VIEW Orders AS "
                        + "SELECT * FROM ("
                        + "    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY update_time DESC) AS row_num "
                        + "    FROM RawOrders"
                        + ") WHERE row_num = 1");

        // Create RawCustomers table
        tableEnv.executeSql(
                "CREATE TABLE RawCustomers ("
                        + "    order_id BIGINT,"
                        + "    customer_id BIGINT,"
                        + "    customer_name STRING,"
                        + "    email STRING,"
                        + "    registration_date TIMESTAMP(3),"
                        + "    loyalty_level STRING,"
                        + "    country STRING,"
                        + "    city STRING,"
                        + "    customer_segment STRING,"
                        + "    update_time AS PROCTIME()"
                        + ") WITH ("
                        + "    'connector' = 'datagen',"
                        + "    'rows-per-second' = '1000',"
                        + "    'fields.order_id.min' = '1',"
                        + "    'fields.order_id.max' = '100000',"
                        + "    'fields.customer_id.min' = '1',"
                        + "    'fields.customer_id.max' = '1000',"
                        + "    'fields.customer_name.length' = '15',"
                        + "    'fields.email.length' = '20',"
                        + "    'fields.loyalty_level.length' = '1',"
                        + "    'fields.country.length' = '3',"
                        + "    'fields.city.length' = '10',"
                        + "    'fields.customer_segment.length' = '1'"
                        + ")");

        // Create deduplication view for Customers
        tableEnv.executeSql(
                "CREATE TEMPORARY VIEW Customers AS "
                        + "SELECT * FROM ("
                        + "    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY update_time DESC) AS row_num "
                        + "    FROM RawCustomers"
                        + ") WHERE row_num = 1");

        // Create RawProducts table
        tableEnv.executeSql(
                "CREATE TABLE RawProducts ("
                        + "    order_id BIGINT,"
                        + "    product_id BIGINT,"
                        + "    product_name STRING,"
                        + "    category_id BIGINT,"
                        + "    brand STRING,"
                        + "    supplier_id BIGINT,"
                        + "    cost DECIMAL(10,2),"
                        + "    weight DECIMAL(8,3),"
                        + "    product_rating DECIMAL(3,2),"
                        + "    is_active BOOLEAN,"
                        + "    update_time AS PROCTIME()"
                        + ") WITH ("
                        + "    'connector' = 'datagen',"
                        + "    'rows-per-second' = '1000',"
                        + "    'fields.order_id.min' = '1',"
                        + "    'fields.order_id.max' = '100000',"
                        + "    'fields.product_id.min' = '1',"
                        + "    'fields.product_id.max' = '5000',"
                        + "    'fields.product_name.length' = '20',"
                        + "    'fields.category_id.min' = '1',"
                        + "    'fields.category_id.max' = '1000',"
                        + "    'fields.brand.length' = '8',"
                        + "    'fields.supplier_id.min' = '1',"
                        + "    'fields.supplier_id.max' = '500',"
                        + "    'fields.cost.min' = '5.00',"
                        + "    'fields.cost.max' = '500.00',"
                        + "    'fields.weight.min' = '0.1',"
                        + "    'fields.weight.max' = '10.0',"
                        + "    'fields.product_rating.min' = '1.0',"
                        + "    'fields.product_rating.max' = '5.0'"
                        + ")");

        // Create deduplication view for Products
        tableEnv.executeSql(
                "CREATE TEMPORARY VIEW Products AS "
                        + "SELECT * FROM ("
                        + "    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY update_time DESC) AS row_num "
                        + "    FROM RawProducts"
                        + ") WHERE row_num = 1");

        // Create RawSuppliers table
        tableEnv.executeSql(
                "CREATE TABLE RawSuppliers ("
                        + "    order_id BIGINT,"
                        + "    supplier_id BIGINT,"
                        + "    supplier_name STRING,"
                        + "    contact_person STRING,"
                        + "    phone STRING,"
                        + "    address STRING,"
                        + "    rating DECIMAL(3,2),"
                        + "    active_since TIMESTAMP(3),"
                        + "    supplier_type STRING,"
                        + "    reliability_score DECIMAL(3,2),"
                        + "    update_time AS PROCTIME()"
                        + ") WITH ("
                        + "    'connector' = 'datagen',"
                        + "    'rows-per-second' = '1000',"
                        + "    'fields.order_id.min' = '1',"
                        + "    'fields.order_id.max' = '100000',"
                        + "    'fields.supplier_id.min' = '1',"
                        + "    'fields.supplier_id.max' = '500',"
                        + "    'fields.supplier_name.length' = '15',"
                        + "    'fields.contact_person.length' = '12',"
                        + "    'fields.phone.length' = '10',"
                        + "    'fields.address.length' = '25',"
                        + "    'fields.rating.min' = '1.0',"
                        + "    'fields.rating.max' = '5.0',"
                        + "    'fields.supplier_type.length' = '1',"
                        + "    'fields.reliability_score.min' = '0.5',"
                        + "    'fields.reliability_score.max' = '1.0'"
                        + ")");

        // Create deduplication view for Suppliers
        tableEnv.executeSql(
                "CREATE TEMPORARY VIEW Suppliers AS "
                        + "SELECT * FROM ("
                        + "    SELECT *, ROW_NUMBER() OVER (PARTITION BY supplier_id ORDER BY update_time DESC) AS row_num "
                        + "    FROM RawSuppliers"
                        + ") WHERE row_num = 1");

        // Create RawCategories table
        tableEnv.executeSql(
                "CREATE TABLE RawCategories ("
                        + "    order_id BIGINT,"
                        + "    category_id BIGINT,"
                        + "    category_name STRING,"
                        + "    description STRING,"
                        + "    parent_category STRING,"
                        + "    is_active BOOLEAN,"
                        + "    created_date TIMESTAMP(3),"
                        + "    category_level INT,"
                        + "    display_order INT,"
                        + "    update_time AS PROCTIME()"
                        + ") WITH ("
                        + "    'connector' = 'datagen',"
                        + "    'rows-per-second' = '1000',"
                        + "    'fields.order_id.min' = '1',"
                        + "    'fields.order_id.max' = '100000',"
                        + "    'fields.category_id.min' = '1',"
                        + "    'fields.category_id.max' = '1000',"
                        + "    'fields.category_name.length' = '12',"
                        + "    'fields.description.length' = '30',"
                        + "    'fields.parent_category.length' = '2',"
                        + "    'fields.category_level.min' = '1',"
                        + "    'fields.category_level.max' = '3',"
                        + "    'fields.display_order.min' = '1',"
                        + "    'fields.display_order.max' = '100'"
                        + ")");

        // Create deduplication view for Categories
        tableEnv.executeSql(
                "CREATE TEMPORARY VIEW Categories AS "
                        + "SELECT * FROM ("
                        + "    SELECT *, ROW_NUMBER() OVER (PARTITION BY category_id ORDER BY update_time DESC) AS row_num "
                        + "    FROM RawCategories"
                        + ") WHERE row_num = 1");

        // Create sink table for results (using blackhole for performance testing)
        tableEnv.executeSql(
                "CREATE TABLE JoinResults ("
                        + "    order_id BIGINT,"
                        + "    customer_name STRING,"
                        + "    product_name STRING,"
                        + "    supplier_name STRING,"
                        + "    category_name STRING,"
                        + "    total_amount DECIMAL(12,2),"
                        + "    profit_margin DECIMAL(10,2),"
                        + "    order_time TIMESTAMP(3),"
                        + "    region STRING,"
                        + "    customer_segment STRING,"
                        + "    supplier_type STRING"
                        + ") WITH ("
                        + "    'connector' = 'blackhole'"
                        + ")");

        System.out.println("=== Executing Multi-Way Join with Composite Conditions ===");
        System.out.println("Joining Orders with Customers, Products, Suppliers, and Categories");
        System.out.println("Using composite join conditions (order_id AND dimension_id)");
        System.out.println("Calculating total amounts and profit margins...\n");

        // Execute the multi-way join query with composite join conditions
        // This demonstrates how key uniqueness is lost through the join chain
        tableEnv.executeSql(
                "INSERT INTO JoinResults "
                        + "SELECT "
                        + "    o.order_id, "
                        + "    c.customer_name, "
                        + "    p.product_name, "
                        + "    s.supplier_name, "
                        + "    cat.category_name, "
                        + "    o.quantity * o.unit_price as total_amount, "
                        + "    (o.quantity * o.unit_price) - (o.quantity * p.cost) as profit_margin, "
                        + "    o.order_time, "
                        + "    o.region, "
                        + "    c.customer_segment, "
                        + "    s.supplier_type "
                        + "FROM Orders o "
                        + "    LEFT JOIN Customers c ON o.order_id = c.order_id AND o.customer_id = c.customer_id "
                        + "    LEFT JOIN Products p ON o.order_id = p.order_id AND o.product_id = p.product_id "
                        + "    LEFT JOIN Suppliers s ON o.order_id = s.order_id AND o.supplier_id = s.supplier_id "
                        + "    LEFT JOIN Categories cat ON o.order_id = cat.order_id AND o.category_id = cat.category_id");

        System.out.println("Multi-join query executed successfully!");
        System.out.println("Results are being discarded (blackhole sink) for performance testing.");
        System.out.println("Check the Flink Web UI for detailed metrics and performance analysis.");
        System.out.println("Press Ctrl+C to stop the streaming job.");

        System.out.println("\n=== Key Features Demonstrated ===");
        System.out.println("1. Deduplication views using ROW_NUMBER() to produce update streams");
        System.out.println("2. Composite join conditions (order_id AND dimension_id)");
        System.out.println("3. Key uniqueness loss through join chain (InputSideHasNoUniqueKey)");
        System.out.println("4. RocksDB state backend with incremental checkpoints");
        System.out.println("5. Unaligned checkpoints for better performance");
        System.out.println("6. Multi-join optimization enabled");
    }
}
