#!/bin/bash

# Reset Kafka Topics Script
# This script drops and recreates the Kafka topics used in the Flink benchmark

echo "=== Kafka Topics Reset Script ==="
echo "Dropping and recreating Kafka topics..."

# Topic names
TENANT_TOPIC="tenant_topic_1kk"
SUPPLIERS_TOPIC="suppliers_topic_1kk"
PRODUCTS_TOPIC="products_topic_1kk"
CATEGORIES_TOPIC="categories_topic_1kk"
ORDERS_TOPIC="orders_topic_1kk"
CUSTOMERS_TOPIC="customers_topic_1kk"
WAREHOUSES_TOPIC="warehouses_topic_1kk"
SHIPPING_TOPIC="shipping_topic_1kk"
PAYMENT_TOPIC="payment_topic_1kk"
INVENTORY_TOPIC="inventory_topic_1kk"
JOIN_RESULTS_MJ_TOPIC="join_results_mj_topic_1kk"
JOIN_RESULTS_BINARY_TOPIC="join_results_binary_topic_1kk"

# Function to check if topic exists
topic_exists() {
    local topic_name=$1
    docker exec -it kafka kafka-topics --bootstrap-server localhost:9093 --list | grep -q "^${topic_name}$"
    return $?
}

# Function to drop topic
drop_topic() {
    local topic_name=$1
    echo "Dropping topic: $topic_name"
    docker exec -it kafka kafka-topics --bootstrap-server localhost:9093 --delete --topic "$topic_name" --if-exists
    if [ $? -eq 0 ]; then
        echo "✓ Successfully dropped topic: $topic_name"
    else
        echo "⚠ Topic $topic_name was not found or could not be dropped"
    fi
}

# Function to create topic
create_topic() {
    local topic_name=$1
    local partitions=${2:-1}
    local replication_factor=${3:-1}
    
    echo "Creating topic: $topic_name (partitions: $partitions, replication: $replication_factor)"
    docker exec -it kafka kafka-topics --bootstrap-server localhost:9093 --create --topic "$topic_name" --partitions "$partitions" --replication-factor "$replication_factor"
    if [ $? -eq 0 ]; then
        echo "✓ Successfully created topic: $topic_name"
    else
        echo "✗ Failed to create topic: $topic_name"
        return 1
    fi
}

# Function to list topics
list_topics() {
    echo ""
    echo "=== Current Kafka Topics ==="
    docker exec -it kafka kafka-topics --bootstrap-server localhost:9093 --list
    echo ""
}

# Main execution
echo "Starting topic reset process..."

# List current topics
# Drop topics
echo "Dropping topics..."
drop_topic "$TENANT_TOPIC"
drop_topic "$SUPPLIERS_TOPIC"
drop_topic "$PRODUCTS_TOPIC"
drop_topic "$CATEGORIES_TOPIC"
drop_topic "$ORDERS_TOPIC"
drop_topic "$CUSTOMERS_TOPIC"
drop_topic "$WAREHOUSES_TOPIC"
drop_topic "$SHIPPING_TOPIC"
drop_topic "$PAYMENT_TOPIC"
drop_topic "$INVENTORY_TOPIC"
drop_topic "$JOIN_RESULTS_MJ_TOPIC"
drop_topic "$JOIN_RESULTS_BINARY_TOPIC"

# Wait a moment for cleanup
#echo "Waiting for cleanup..."
#sleep 2

# Create topics
echo "Creating topics..."
create_topic "$TENANT_TOPIC" 10 1
create_topic "$SUPPLIERS_TOPIC" 10 1
create_topic "$PRODUCTS_TOPIC" 10 1
create_topic "$CATEGORIES_TOPIC" 10 1
create_topic "$ORDERS_TOPIC" 10 1
create_topic "$CUSTOMERS_TOPIC" 10 1
create_topic "$WAREHOUSES_TOPIC" 10 1
create_topic "$SHIPPING_TOPIC" 10 1
create_topic "$PAYMENT_TOPIC" 10 1
create_topic "$INVENTORY_TOPIC" 10 1
create_topic "$JOIN_RESULTS_MJ_TOPIC" 10 1
create_topic "$JOIN_RESULTS_BINARY_TOPIC" 10 1

# List final topics
echo "Topics after reset:"
list_topics

echo "=== Topic Reset Complete ==="
echo "You can now run your Flink SQL job!" 
