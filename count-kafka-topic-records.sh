#!/bin/bash

# Script to count records in a Kafka topic.
# It calculates the total number of messages by summing the end offsets of all partitions
# and subtracting the sum of the beginning offsets.

set -e
set -o pipefail

if [ -z "$1" ]; then
    echo "Usage: $0 <topic_name>"
    echo "This script counts the number of records in a given Kafka topic."
    exit 1
fi

TOPIC_NAME="$1"
BOOTSTRAP_SERVER="localhost:9093"
KAFKA_CONTAINER="kafka"

echo "Counting records for topic: $TOPIC_NAME"

# Check if docker container is running
if ! docker ps | grep -q $KAFKA_CONTAINER; then
  echo "Error: Docker container '$KAFKA_CONTAINER' not found or not running."
  exit 1
fi

# Get the latest offsets for all partitions (log end offsets)
LATEST_OFFSETS_STR=$(docker exec "$KAFKA_CONTAINER" kafka-run-class kafka.tools.GetOffsetShell --broker-list "$BOOTSTRAP_SERVER" --topic "$TOPIC_NAME" --time -1)
if [ -z "$LATEST_OFFSETS_STR" ]; then
    echo "Could not retrieve latest offsets for topic '$TOPIC_NAME'. The topic might not exist."
    exit 1
fi

# Get the earliest offsets for all partitions (log start offsets)
EARLIEST_OFFSETS_STR=$(docker exec "$KAFKA_CONTAINER" kafka-run-class kafka.tools.GetOffsetShell --broker-list "$BOOTSTRAP_SERVER" --topic "$TOPIC_NAME" --time -2)
if [ -z "$EARLIEST_OFFSETS_STR" ]; then
    echo "Could not retrieve earliest offsets for topic '$TOPIC_NAME'. The topic might not exist."
    exit 1
fi

LATEST_SUM=$(echo "$LATEST_OFFSETS_STR" | awk -F: '{sum += $3} END {print sum}')
EARLIEST_SUM=$(echo "$EARLIEST_OFFSETS_STR" | awk -F: '{sum += $3} END {print sum}')

# It's possible for sums to be empty if awk fails or there's no output
LATEST_SUM=${LATEST_SUM:-0}
EARLIEST_SUM=${EARLIEST_SUM:-0}

TOTAL_RECORDS=$((LATEST_SUM - EARLIEST_SUM))

echo "Total records in topic '$TOPIC_NAME': $TOTAL_RECORDS"
