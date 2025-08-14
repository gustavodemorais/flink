-- Flink Join Benchmark: Binary vs Multi-way Joins
-- Set execution mode to streaming for continuous data generation
SET
  'execution.runtime-mode' = 'streaming';

-- Set result mode to changelog for streaming results
--SET
--  'sql-client.execution.result-mode' = 'changelog';

SET
  'sql-client.execution.result-mode' = 'tableau';

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


SET
'table.optimizer.multi-join.enabled' = 'true';


CREATE TEMPORARY TABLE heros (
  `name` STRING,
  `power` STRING,
  `age` INT
) WITH (
  'connector' = 'faker',
  'fields.name.expression' = '#{superhero.name}',
  'fields.power.expression' = '#{superhero.power}',
  'fields.power.null-rate' = '0.05',
  'fields.age.expression' = '#{number.numberBetween ''0'',''1000''}'
);

SELECT * FROM heros;
