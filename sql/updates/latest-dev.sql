
-- add fields for check function
ALTER TABLE _timescaledb_config.bgw_job ADD COLUMN check_schema NAME;
ALTER TABLE _timescaledb_config.bgw_job ADD COLUMN check_name NAME;

-- no need to touch the telemetry jobs since they do not have a check
-- function.

-- add check function to reorder jobs
UPDATE
  _timescaledb_config.bgw_job job
SET
  check_schema = '_timescaledb_internal',
  check_name = 'policy_reorder_check'
WHERE proc_schema = '_timescaledb_internal'
  AND proc_name = 'policy_reorder';

-- add check function to compression jobs
UPDATE
  _timescaledb_config.bgw_job job
SET
  check_schema = '_timescaledb_internal',
  check_name = 'policy_compression_check'
WHERE proc_schema = '_timescaledb_internal'
  AND proc_name = 'policy_compression';

-- add check function to retention jobs
UPDATE
  _timescaledb_config.bgw_job job
SET
  check_schema = '_timescaledb_internal',
  check_name = 'policy_retention_check'
WHERE proc_schema = '_timescaledb_internal'
  AND proc_name = 'policy_retention';

-- add check function to continuous aggregate refresh jobs
UPDATE
  _timescaledb_config.bgw_job job
SET
  check_schema = '_timescaledb_internal',
  check_name = 'policy_check_continuous_aggregate'
WHERE proc_schema = '_timescaledb_internal'
  AND proc_name = 'policy_refresh_continuous_aggregate';