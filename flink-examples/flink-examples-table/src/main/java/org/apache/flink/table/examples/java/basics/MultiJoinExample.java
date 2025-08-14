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
 * Multi-Join Example demonstrating how to perform complex multi-way joins in Flink SQL.
 *
 * <p>This example shows: 1. Setting up streaming execution with proper configuration 2. Creating
 * multiple tables with data generation 3. Performing multi-way joins between Orders, Customers,
 * Products, Suppliers, and Categories 4. Calculating business metrics like total amount and profit
 * margin 5. Demonstrating the difference between binary joins and multi-join optimization
 */
public final class MultiJoinExample {

    public static void main(String[] args) throws Exception {

        // Set up the Table API with streaming mode
        final EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inStreamingMode().build();
        final TableEnvironment tableEnv = TableEnvironment.create(settings);

        // Configure execution settings for optimal performance
        tableEnv.getConfig().set("parallelism.default", "4");
        tableEnv.getConfig().set("pipeline.max-parallelism", "4");
        tableEnv.getConfig().set("execution.checkpointing.interval", "10s");
        tableEnv.getConfig().set("execution.checkpointing.timeout", "5min");
        tableEnv.getConfig().set("execution.checkpointing.min-pause", "2s");
        tableEnv.getConfig().set("execution.checkpointing.max-concurrent-checkpoints", "1");
        tableEnv.getConfig().set("execution.checkpointing.mode", "EXACTLY_ONCE");
        tableEnv.getConfig().set("execution.checkpointing.storage", "filesystem");
        tableEnv.getConfig().set("state.checkpoints.dir", "file:///tmp/flink-checkpoints");
        tableEnv.getConfig().set("taskmanager.debug.mode", "true");
        tableEnv.getConfig().set("table.optimizer.multi-join.enabled", "true");

        // Create Orders table (fact table) with data generation
        tableEnv.executeSql(
                "CREATE TABLE Orders ("
                        + "    order_id INT NOT NULL,"
                        + "    customer_id BIGINT,"
                        + "    product_id BIGINT,"
                        + "    supplier_id BIGINT,"
                        + "    category_id STRING,"
                        + "    quantity INT,"
                        + "    unit_price DECIMAL(10,2),"
                        + "    order_time TIMESTAMP(3),"
                        + "    region STRING,"
                        + "    payment_method STRING,"
                        + "    order_status STRING,"
                        + "    PRIMARY KEY (order_id) NOT ENFORCED"
                        + ") WITH ("
                        + "    'connector' = 'datagen',"
                        + "    'rows-per-second' = '1000',"
                        + "    'fields.order_id.kind' = 'sequence',"
                        + "    'fields.order_id.start' = '1',"
                        + "    'fields.order_id.end' = '200',"
                        + "    'fields.customer_id.min' = '1',"
                        + "    'fields.customer_id.max' = '1000',"
                        + "    'fields.product_id.min' = '1',"
                        + "    'fields.product_id.max' = '5000',"
                        + "    'fields.supplier_id.min' = '1',"
                        + "    'fields.supplier_id.max' = '500',"
                        + "    'fields.category_id.length' = '2',"
                        + "    'fields.quantity.min' = '1',"
                        + "    'fields.quantity.max' = '10',"
                        + "    'fields.unit_price.min' = '10.00',"
                        + "    'fields.unit_price.max' = '1000.00',"
                        + "    'fields.region.length' = '3',"
                        + "    'fields.payment_method.length' = '2',"
                        + "    'fields.order_status.length' = '1'"
                        + ")");

        // Create Customers dimension table
        tableEnv.executeSql(
                "CREATE TABLE Customers ("
                        + "    order_id INT NOT NULL,"
                        + "    customer_id BIGINT,"
                        + "    customer_name STRING,"
                        + "    email STRING,"
                        + "    registration_date TIMESTAMP(3),"
                        + "    loyalty_level STRING,"
                        + "    country STRING,"
                        + "    city STRING,"
                        + "    customer_segment STRING,"
                        + "    PRIMARY KEY (customer_id) NOT ENFORCED"
                        + ") WITH ("
                        + "    'connector' = 'datagen',"
                        + "    'rows-per-second' = '800',"
                        + "    'fields.order_id.min' = '1',"
                        + "    'fields.order_id.max' = '200',"
                        + "    'fields.customer_id.min' = '1',"
                        + "    'fields.customer_id.max' = '1000',"
                        + "    'fields.customer_name.length' = '15',"
                        + "    'fields.email.length' = '20',"
                        + "    'fields.loyalty_level.length' = '1',"
                        + "    'fields.country.length' = '3',"
                        + "    'fields.city.length' = '10',"
                        + "    'fields.customer_segment.length' = '1'"
                        + ")");

        // Create Products dimension table
        tableEnv.executeSql(
                "CREATE TABLE Products ("
                        + "    order_id INT NOT NULL,"
                        + "    product_id BIGINT,"
                        + "    product_name STRING,"
                        + "    category_id STRING,"
                        + "    brand STRING,"
                        + "    supplier_id BIGINT,"
                        + "    cost DECIMAL(10,2),"
                        + "    weight DECIMAL(8,3),"
                        + "    product_rating DECIMAL(3,2),"
                        + "    is_active BOOLEAN,"
                        + "    PRIMARY KEY (product_id) NOT ENFORCED"
                        + ") WITH ("
                        + "    'connector' = 'datagen',"
                        + "    'rows-per-second' = '600',"
                        + "    'fields.order_id.min' = '1',"
                        + "    'fields.order_id.max' = '200',"
                        + "    'fields.product_id.min' = '1',"
                        + "    'fields.product_id.max' = '5000',"
                        + "    'fields.product_name.length' = '20',"
                        + "    'fields.category_id.length' = '2',"
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

        // Create Suppliers dimension table
        tableEnv.executeSql(
                "CREATE TABLE Suppliers ("
                        + "    order_id INT NOT NULL,"
                        + "    supplier_id BIGINT,"
                        + "    supplier_name STRING,"
                        + "    contact_person STRING,"
                        + "    phone STRING,"
                        + "    address STRING,"
                        + "    rating DECIMAL(3,2),"
                        + "    active_since TIMESTAMP(3),"
                        + "    supplier_type STRING,"
                        + "    reliability_score DECIMAL(3,2),"
                        + "    PRIMARY KEY (supplier_id) NOT ENFORCED"
                        + ") WITH ("
                        + "    'connector' = 'datagen',"
                        + "    'rows-per-second' = '400',"
                        + "    'fields.order_id.min' = '1',"
                        + "    'fields.order_id.max' = '200',"
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

        // Create Categories dimension table
        tableEnv.executeSql(
                "CREATE TABLE Categories ("
                        + "    order_id INT NOT NULL,"
                        + "    category_id STRING,"
                        + "    category_name STRING,"
                        + "    description STRING,"
                        + "    parent_category STRING,"
                        + "    is_active BOOLEAN,"
                        + "    created_date TIMESTAMP(3),"
                        + "    category_level INT,"
                        + "    display_order INT,"
                        + "    PRIMARY KEY (category_id) NOT ENFORCED"
                        + ") WITH ("
                        + "    'connector' = 'datagen',"
                        + "    'rows-per-second' = '300',"
                        + "    'fields.order_id.min' = '1',"
                        + "    'fields.order_id.max' = '200',"
                        + "    'fields.category_id.length' = '2',"
                        + "    'fields.category_name.length' = '12',"
                        + "    'fields.description.length' = '30',"
                        + "    'fields.parent_category.length' = '2',"
                        + "    'fields.category_level.min' = '1',"
                        + "    'fields.category_level.max' = '3',"
                        + "    'fields.display_order.min' = '1',"
                        + "    'fields.display_order.max' = '100'"
                        + ")");

        // Create sink table for results
        tableEnv.executeSql(
                "CREATE TABLE JoinResults ("
                        + "    order_id INT NOT NULL,"
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
                        + "    'connector' = 'print'"
                        + ")");

        System.out.println("=== Multi-Join Example: E-commerce Analytics ===");
        System.out.println("Joining Orders with Customers, Products, Suppliers, and Categories");
        System.out.println("Calculating total amounts and profit margins...\n");

        // Execute the multi-way join query
        // This demonstrates joining 5 tables in a single query with business logic
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
                        + "    LEFT JOIN Customers c ON o.order_id = c.order_id "
                        + "    LEFT JOIN Products p ON o.order_id = p.order_id "
                        + "    LEFT JOIN Suppliers s ON o.order_id = s.order_id "
                        + "    LEFT JOIN Categories cat ON o.order_id = cat.order_id");

        System.out.println("Multi-join query executed successfully!");
        System.out.println("Results are being printed to the console.");
        System.out.println("Press Ctrl+C to stop the streaming job.");
    }
}
