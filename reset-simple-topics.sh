#!/bin/bash

# Reset Kafka Topics Script for Simple Join Example
# This script drops and recreates the Kafka topics used in the simple join example

echo "=== Simple Join Example - Kafka Topics Reset Script ==="
echo "Dropping and recreating Kafka topics..."

# Topic names
USER_TOPIC="user_topic"
ORDER_TOPIC="order_topic"
JOIN_RESULTS_TOPIC="join_results_topic"

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
echo "Current topics before reset:"
list_topics

# Drop topics
echo "Dropping topics..."
drop_topic "$USER_TOPIC"
drop_topic "$ORDER_TOPIC"
drop_topic "$JOIN_RESULTS_TOPIC"

# Wait a moment for cleanup
echo "Waiting for cleanup..."
sleep 2

# Create topics
echo "Creating topics..."
create_topic "$USER_TOPIC" 1 1
create_topic "$ORDER_TOPIC" 1 1
create_topic "$JOIN_RESULTS_TOPIC" 1 1

# List final topics
echo "Topics after reset:"
list_topics

echo "=== Topic Reset Complete ==="
echo "You can now run your simple join example!"
echo ""
echo "To monitor the results:"
echo "  # Monitor user data:"
echo "  docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9093 --topic user_topic --from-beginning"
echo ""
echo "  # Monitor order data:"
echo "  docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9093 --topic order_topic --from-beginning"
echo ""
echo "  # Monitor join results:"
echo "  docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9093 --topic join_results_topic --from-beginning" 
