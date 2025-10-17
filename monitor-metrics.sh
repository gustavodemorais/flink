OUTPUT_FILE="metrics-log.txt"

echo "timestamp,job_runtime_sec,total_rec_per_sec,checkpoint_mb,heap_pct,gc_sec,managed_mb,managed_used_mb,managed_pct,backpressure,rocksdb_compactions,rocksdb_pending_comp_mb,rocksdb_mem_tables_mb,rocksdb_stall_ms,rocksdb_write_stopped,rocksdb_flushes,rocksdb_immutable_mems,rocksdb_flush_pending,rocksdb_live_data_mb,rocksdb_cache_hit_rate_pct" > "$OUTPUT_FILE"
echo "Starting enhanced metric collection with write throttling tracking. Logging to $OUTPUT_FILE"
echo "Press Ctrl+C to stop"
echo ""

while true; do
  TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')

  # Get job and IDs
  JOB_ID=$(curl -s http://localhost:8081/jobs | jq -r '.jobs[0].id')
  JOB_DETAILS=$(curl -s "http://localhost:8081/jobs/$JOB_ID")
  VERTEX_ID=$(echo "$JOB_DETAILS" | jq -r '.vertices[] | select(.name | contains("Writer")) | .id' | head -1)
  TM_ID=$(curl -s "http://localhost:8081/taskmanagers" | jq -r '.taskmanagers[0].id')

  # Get job runtime (convert milliseconds to seconds)
  JOB_DURATION_MS=$(echo "$JOB_DETAILS" | jq -r '.duration // 0')
  JOB_RUNTIME_SEC=$(awk "BEGIN {printf \"%.0f\", $JOB_DURATION_MS/1000}")
  
  # Format runtime as HH:MM:SS for display
  RUNTIME_HOURS=$((JOB_RUNTIME_SEC / 3600))
  RUNTIME_MINS=$(((JOB_RUNTIME_SEC % 3600) / 60))
  RUNTIME_SECS=$((JOB_RUNTIME_SEC % 60))
  RUNTIME_FORMATTED=$(printf "%02d:%02d:%02d" $RUNTIME_HOURS $RUNTIME_MINS $RUNTIME_SECS)

  # Get total throughput (sum across all subtasks) - handle nulls
  TOTAL_RECORDS_PER_SEC=$(curl -s "http://localhost:8081/jobs/$JOB_ID/vertices/$VERTEX_ID/subtasks/metrics?get=numRecordsInPerSecond" | jq '.[0].sum // 0 | floor')

  # Checkpoint size - handle null
  CHECKPOINT_SIZE_BYTES=$(curl -s "http://localhost:8081/jobs/$JOB_ID/checkpoints" | jq -r '.latest.completed.state_size // 0')
  if [ "$CHECKPOINT_SIZE_BYTES" = "0" ] || [ -z "$CHECKPOINT_SIZE_BYTES" ]; then
    CHECKPOINT_SIZE_MB="0"
  else
    CHECKPOINT_SIZE_MB=$(awk "BEGIN {printf \"%.2f\", $CHECKPOINT_SIZE_BYTES/1048576}")
  fi

  # Get TaskManager details (contains embedded metrics)
  TM_DETAILS=$(curl -s "http://localhost:8081/taskmanagers/$TM_ID")

  # Heap usage from embedded metrics
  HEAP_PCT=$(echo "$TM_DETAILS" | jq '(.metrics.heapUsed / .metrics.heapMax * 100) | floor')

  # GC time from embedded metrics (find "All" collector, convert ms to seconds)
  GC_TIME=$(echo "$TM_DETAILS" | jq '.metrics.garbageCollectors[] | select(.name == "All") | (.time / 1000) | floor')

  # Managed memory from memoryConfiguration (convert to MB)
  MANAGED_MB=$(echo "$TM_DETAILS" | jq '(.memoryConfiguration.managedMemory / 1048576) | floor')
  
  # Managed memory used (requires separate metric query)
  MANAGED_USED=$(curl -s "http://localhost:8081/taskmanagers/$TM_ID/metrics?get=Status.Flink.Memory.Managed.Used" | jq -r '.[0].value | tonumber')
  MANAGED_USED_MB=$(awk "BEGIN {printf \"%.2f\", $MANAGED_USED/1048576}")
  MANAGED_PCT=$(awk "BEGIN {printf \"%.0f\", ($MANAGED_USED/1048576/$MANAGED_MB)*100}")

  # Backpressure
  BACKPRESSURE=$(curl -s "http://localhost:8081/jobs/$JOB_ID/vertices/$VERTEX_ID/backpressure" 2>/dev/null | jq -r '.status // "N/A"')

  # Find Join operator for RocksDB metrics (get the last one, closest to output)
  JOIN_VERTEX_ID=$(curl -s "http://localhost:8081/jobs/$JOB_ID" | jq -r '.vertices[] | select(.name | contains("Join")) | .id' | tail -1)

  # RocksDB metrics - handle nulls
  if [ -n "$JOIN_VERTEX_ID" ] && [ "$JOIN_VERTEX_ID" != "null" ]; then
    # Get parallelism for the Join vertex
    JOIN_PARALLELISM=$(curl -s "http://localhost:8081/jobs/$JOB_ID" | jq -r ".vertices[] | select(.id == \"$JOIN_VERTEX_ID\") | .parallelism")
    
    # Fetch metrics from all subtasks (RocksDB metrics are per-subtask)
    ROCKSDB_COMPACTIONS=0
    ROCKSDB_PENDING_COMP=0
    ROCKSDB_MEM_TABLES=0
    ROCKSDB_MEM_IMMUTABLE=0
    ROCKSDB_FLUSH_PENDING=0
    ROCKSDB_STALL_MICROS=0
    ROCKSDB_WRITE_STOPPED=0
    ROCKSDB_NUM_FLUSHES=0
    ROCKSDB_LIVE_DATA=0
    ROCKSDB_CACHE_HITS=0
    ROCKSDB_CACHE_MISSES=0
    
    # Get the metric IDs from first subtask to build query (URL-encode them)
    METRIC_IDS=$(curl -s "http://localhost:8081/jobs/$JOB_ID/vertices/$JOIN_VERTEX_ID/subtasks/0/metrics" | jq -r '[.[] | select(.id | contains("rocksdb")) | .id | @uri] | join(",")')
    
    for ((i=0; i<$JOIN_PARALLELISM; i++)); do
      SUBTASK_METRICS=$(curl -s "http://localhost:8081/jobs/$JOB_ID/vertices/$JOIN_VERTEX_ID/subtasks/$i/metrics?get=$METRIC_IDS")
      
      # Sum metrics across column families and subtasks
      VAL=$(echo "$SUBTASK_METRICS" | jq '[.[] | select(.id | contains("rocksdb_num-running-compactions")) | (.value | tonumber)] | add // 0')
      ROCKSDB_COMPACTIONS=$((ROCKSDB_COMPACTIONS + ${VAL:-0}))
      
      VAL=$(echo "$SUBTASK_METRICS" | jq '[.[] | select(.id | contains("rocksdb_estimate-pending-compaction-bytes")) | (.value | tonumber)] | add // 0')
      ROCKSDB_PENDING_COMP=$((ROCKSDB_PENDING_COMP + ${VAL:-0}))
      
      VAL=$(echo "$SUBTASK_METRICS" | jq '[.[] | select(.id | contains("rocksdb_cur-size-all-mem-tables")) | (.value | tonumber)] | add // 0')
      ROCKSDB_MEM_TABLES=$((ROCKSDB_MEM_TABLES + ${VAL:-0}))
      
      VAL=$(echo "$SUBTASK_METRICS" | jq '[.[] | select(.id | contains("rocksdb_num-immutable-mem-table")) | (.value | tonumber)] | add // 0')
      ROCKSDB_MEM_IMMUTABLE=$((ROCKSDB_MEM_IMMUTABLE + ${VAL:-0}))
      
      VAL=$(echo "$SUBTASK_METRICS" | jq '[.[] | select(.id | contains("rocksdb_mem-table-flush-pending")) | (.value | tonumber)] | add // 0')
      ROCKSDB_FLUSH_PENDING=$((ROCKSDB_FLUSH_PENDING + ${VAL:-0}))
      
      VAL=$(echo "$SUBTASK_METRICS" | jq '[.[] | select(.id | contains("rocksdb_num-running-flushes")) | (.value | tonumber)] | add // 0')
      ROCKSDB_NUM_FLUSHES=$((ROCKSDB_NUM_FLUSHES + ${VAL:-0}))
      
      VAL=$(echo "$SUBTASK_METRICS" | jq '[.[] | select(.id | contains("rocksdb_estimate-live-data-size")) | (.value | tonumber)] | add // 0')
      ROCKSDB_LIVE_DATA=$((ROCKSDB_LIVE_DATA + ${VAL:-0}))
      
      VAL=$(echo "$SUBTASK_METRICS" | jq '[.[] | select(.id | contains("rocksdb_block_cache_hit")) | (.value | tonumber)] | add // 0')
      ROCKSDB_CACHE_HITS=$((ROCKSDB_CACHE_HITS + ${VAL:-0}))
      
      VAL=$(echo "$SUBTASK_METRICS" | jq '[.[] | select(.id | contains("rocksdb_block_cache_miss")) | (.value | tonumber)] | add // 0')
      ROCKSDB_CACHE_MISSES=$((ROCKSDB_CACHE_MISSES + ${VAL:-0}))
    done
    
    # Convert bytes to MB
    if [ "$ROCKSDB_PENDING_COMP" = "0" ]; then
      ROCKSDB_PENDING_COMP_MB="0"
    else
      ROCKSDB_PENDING_COMP_MB=$(awk "BEGIN {printf \"%.2f\", $ROCKSDB_PENDING_COMP/1048576}")
    fi
    
    if [ "$ROCKSDB_MEM_TABLES" = "0" ]; then
      ROCKSDB_MEM_TABLES_MB="0"
    else
      ROCKSDB_MEM_TABLES_MB=$(awk "BEGIN {printf \"%.2f\", $ROCKSDB_MEM_TABLES/1048576}")
    fi
    
    if [ "$ROCKSDB_LIVE_DATA" = "0" ]; then
      ROCKSDB_LIVE_DATA_MB="0"
    else
      ROCKSDB_LIVE_DATA_MB=$(awk "BEGIN {printf \"%.2f\", $ROCKSDB_LIVE_DATA/1048576}")
    fi

    # Convert stall time to milliseconds
    STALL_MS=$(awk "BEGIN {printf \"%.2f\", $ROCKSDB_STALL_MICROS/1000}")
    
    # Calculate cache hit rate
    TOTAL_CACHE_ACCESSES=$((ROCKSDB_CACHE_HITS + ROCKSDB_CACHE_MISSES))
    if [ "$TOTAL_CACHE_ACCESSES" -gt 0 ]; then
      CACHE_HIT_RATE=$(awk "BEGIN {printf \"%.1f\", ($ROCKSDB_CACHE_HITS/$TOTAL_CACHE_ACCESSES)*100}")
    else
      CACHE_HIT_RATE="N/A"
    fi
  else
    ROCKSDB_COMPACTIONS="0"
    ROCKSDB_PENDING_COMP_MB="0"
    ROCKSDB_MEM_TABLES_MB="0"
    ROCKSDB_MEM_IMMUTABLE="0"
    ROCKSDB_FLUSH_PENDING="0"
    ROCKSDB_STALL_MICROS="0"
    STALL_MS="0"
    ROCKSDB_WRITE_STOPPED="0"
    ROCKSDB_NUM_FLUSHES="0"
    ROCKSDB_LIVE_DATA_MB="0"
    CACHE_HIT_RATE="N/A"
  fi

  # CSV format
  echo "$TIMESTAMP,$JOB_RUNTIME_SEC,$TOTAL_RECORDS_PER_SEC,$CHECKPOINT_SIZE_MB,$HEAP_PCT,$GC_TIME,$MANAGED_MB,$MANAGED_USED_MB,$MANAGED_PCT,$BACKPRESSURE,$ROCKSDB_COMPACTIONS,$ROCKSDB_PENDING_COMP_MB,$ROCKSDB_MEM_TABLES_MB,$STALL_MS,$ROCKSDB_WRITE_STOPPED,$ROCKSDB_NUM_FLUSHES,$ROCKSDB_MEM_IMMUTABLE,$ROCKSDB_FLUSH_PENDING,$ROCKSDB_LIVE_DATA_MB,$CACHE_HIT_RATE" >> "$OUTPUT_FILE"

  # Human-readable output with write throttling indicators
  THROTTLE_INDICATOR=""
  if [ "$ROCKSDB_WRITE_STOPPED" = "1" ]; then
    THROTTLE_INDICATOR="🔴 WRITE STOPPED! "
  elif [ "$ROCKSDB_STALL_MICROS" != "0" ] && [ -n "$ROCKSDB_STALL_MICROS" ]; then
    THROTTLE_INDICATOR="⚠️ THROTTLED! "
  fi

  echo "$TIMESTAMP | Runtime: $RUNTIME_FORMATTED | Rec/s: $TOTAL_RECORDS_PER_SEC | Chkpt: ${CHECKPOINT_SIZE_MB}MB | Heap: ${HEAP_PCT}% | GC: ${GC_TIME}s | Managed: ${MANAGED_USED_MB}/${MANAGED_MB}MB (${MANAGED_PCT}%) | BP: $BACKPRESSURE | ${THROTTLE_INDICATOR}RocksDB: Comp=$ROCKSDB_COMPACTIONS Flush=$ROCKSDB_NUM_FLUSHES Stall=${STALL_MS}ms ImmMem=$ROCKSDB_MEM_IMMUTABLE FlushPend=$ROCKSDB_FLUSH_PENDING MemTbl=${ROCKSDB_MEM_TABLES_MB}MB LiveData=${ROCKSDB_LIVE_DATA_MB}MB CacheHit=${CACHE_HIT_RATE}%"

  sleep 2
done
