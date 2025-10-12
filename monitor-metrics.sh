#!/bin/bash

OUTPUT_FILE="metrics-log.txt"
JOB_ID=$(curl -s http://localhost:8081/jobs | jq -r '.jobs[0].id')
VERTEX_ID=$(curl -s "http://localhost:8081/jobs/$JOB_ID" | jq -r ".vertices[] | select(.name | contains(\"Writer\")) | .id" | head -1)

METRICS_ENDPOINT="http://localhost:8081/jobs/$JOB_ID/vertices/$VERTEX_ID/metrics?get=0.numRecordsInPerSecond,1.numRecordsInPerSecond,2.numRecordsInPerSecond,3.numRecordsInPerSecond,4.numRecordsInPerSecond,5.numRecordsInPerSecond,6.numRecordsInPerSecond,7.numRecordsInPerSecond,8.numRecordsInPerSecond,9.numRecordsInPerSecond,10.numRecordsInPerSecond"
CHECKPOINT_ENDPOINT="http://localhost:8081/jobs/$JOB_ID/checkpoints"

echo "Starting metric collection. Logging to $OUTPUT_FILE"
echo "Press Ctrl+C to stop"
echo ""

while true; do
  TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')

  # Get metrics
  METRICS=$(curl -s "$METRICS_ENDPOINT")

  # Get checkpoint info
  CHECKPOINT_SIZE=$(curl -s "$CHECKPOINT_ENDPOINT" | jq -r '.latest.completed.state_size // "N/A"')

# Convert to MB
  if [ -z "$CHECKPOINT_SIZE" ]; then
    CHECKPOINT_SIZE_MB="N/A"
  else
    CHECKPOINT_SIZE_MB=$(awk "BEGIN {printf \"%.2f\", $CHECKPOINT_SIZE/1048576}")
  fi

  # Log everything
  echo "$TIMESTAMP -> Metrics: $METRICS | Checkpoint Size: $CHECKPOINT_SIZE_MB MB" >> "$OUTPUT_FILE"
  echo "$TIMESTAMP -> Metrics: $METRICS | Checkpoint Size: $CHECKPOINT_SIZE_MB MB"

  sleep 2
done
