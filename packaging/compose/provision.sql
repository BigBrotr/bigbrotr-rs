-- Explicit operator provisioning of a fresh, dedicated PostgreSQL instance.
-- Run as postgres, with -X -v ON_ERROR_STOP=1 -v stage=<stage>.
-- No policy/principal seed rows, replication bypass or application ownership.
\set ON_ERROR_STOP on
SELECT 1 / ((:'stage' IN ('bootstrap', 'control', 'reader'))::integer);
SELECT :'stage' = 'bootstrap' AS bootstrap,
       :'stage' = 'control' AS control,
       :'stage' = 'reader' AS reader \gset

\if :bootstrap
\set migrator_password `cat /run/operator/migrator`
\set control_password `cat /run/secrets/control`
\set catalog_password `cat /run/secrets/catalog`
\set reader_password `cat /run/secrets/archive-read`
SELECT 1 / ((:'migrator_password' ~ '^[0-9a-f]{64}$'
    AND :'control_password' ~ '^[0-9a-f]{64}$'
    AND :'catalog_password' ~ '^[0-9a-f]{64}$'
    AND :'reader_password' ~ '^[0-9a-f]{64}$')::integer);
CREATE ROLE bigbrotr_migrator LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
    NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD :'migrator_password';
CREATE ROLE bigbrotr_control LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
    NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD :'control_password';
CREATE ROLE bigbrotr_catalog LOGIN NOSUPERUSER NOCREATEDB CREATEROLE
    NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD :'catalog_password';
CREATE ROLE bigbrotr_archive_read LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
    NOINHERIT NOREPLICATION NOBYPASSRLS PASSWORD :'reader_password';
CREATE DATABASE bigbrotr_control OWNER bigbrotr_migrator;
CREATE DATABASE bigbrotr_catalog OWNER postgres;
REVOKE ALL ON DATABASE bigbrotr_control, bigbrotr_catalog FROM PUBLIC;
GRANT CONNECT ON DATABASE bigbrotr_control TO bigbrotr_control;
\connect bigbrotr_control
REVOKE ALL ON SCHEMA public FROM PUBLIC;
\connect bigbrotr_catalog
REVOKE ALL ON SCHEMA public FROM PUBLIC;
\endif

\if :control
\connect bigbrotr_control
-- Static proposal for fresh PostgreSQL Control target40, applied by the
-- trusted provisioning owner after real migrate apply. Not engine-qualified.
-- The application role must already be LOGIN NOSUPERUSER NOCREATEDB
-- NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS, with no memberships,
-- schema/table/function ownership, schema CREATE, or database CREATE/TEMP.
-- This file grants no schema_versions write, DELETE beyond session/proof cleanup,
-- TRUNCATE, REFERENCES, TRIGGER, DDL, parameter SET, or grant option.
-- Initial Control/Ingest/Sync/Archive + ordinary read/export + ingest recovery
-- and successor flows. Cursor initialization is included; optional key lifecycle,
-- job-page cursor retention, export resume, pin recovery and import writes are not.

GRANT CONNECT ON DATABASE bigbrotr_control TO bigbrotr_control;
GRANT USAGE ON SCHEMA control TO bigbrotr_control;
-- Current target40 catalog only; no ALTER DEFAULT PRIVILEGES.
GRANT SELECT ON ALL TABLES IN SCHEMA control TO bigbrotr_control;

GRANT INSERT ON
    control.datasets,
    control.config_revisions,
    control.principals,
    control.management_audit,
    control.policy_revisions,
    control.management_commands,
    control.local_accounts,
    control.local_credentials,
    control.principal_grants,
    control.owner_challenges,
    control.role_instances,
    control.owner_epochs,
    control.topology_generations,
    control.topology_streams,
    control.allocation_domains,
    control.source_streams,
    control.producer_slots,
    control.producer_epochs,
    control.producer_requests,
    control.producer_outcomes,
    control.producer_input_bindings,
    control.producer_checkpoints,
    control.producer_checkpoint_dependencies,
    control.acquisition_progress,
    control.browser_sessions,
    control.login_buckets,
    control.reauthentication_proofs,
    control.relay_endpoints,
    control.relay_access_contexts,
    control.network_routes,
    control.relay_enrollments,
    control.relay_enrollment_changes,
    control.relay_registration_commands,
    control.acquisition_selectors,
    control.acquisition_scopes,
    control.acquisition_scope_commands,
    control.jobs,
    control.acquisition_jobs,
    control.job_admissions,
    control.job_input_bounds,
    control.archive_coordinators,
    control.archive_persistence,
    control.archive_attempts,
    control.archive_owner_sessions,
    control.consumers,
    control.consumer_frontier_revisions,
    control.consumer_frontiers,
    control.archive_frontier_snapshots,
    control.archive_frontier_batches,
    control.snapshot_leases,
    control.archive_snapshot_pins,
    control.retention_claims,
    control.scope_grants,
    control.scope_grant_changes,
    control.export_jobs,
    control.export_execution_units,
    control.artifacts,
    control.artifact_files,
    control.initial_ingest_successions,
    control.initial_ingest_activations,
    control.acquisition_gaps,
    control.ingest_successions,
    control.ingest_activations,
    control.ingest_terminal_transactions,
    control.cursor_keyrings,
    control.cursor_keys,
    control.cursor_key_materials
    TO bigbrotr_control;

-- Table-level UPDATE is required by several current startup preflights.
-- Immutable history triggers still reject prohibited content mutation.
GRANT UPDATE ON
    control.role_instances,
    control.owner_epochs,
    control.topology_generations,
    control.allocation_domains,
    control.source_streams,
    control.producer_epochs,
    control.producer_checkpoints,
    control.acquisition_progress,
    control.browser_sessions,
    control.login_buckets,
    control.local_accounts,
    control.local_credentials,
    control.relay_enrollments,
    control.jobs,
    control.consumers,
    control.consumer_frontier_revisions,
    control.snapshot_leases,
    control.retention_claims,
    control.scope_grants,
    control.initial_ingest_successions
    TO bigbrotr_control;

GRANT UPDATE(config_revision,policy_revision,topology_generation)
    ON control.datasets TO bigbrotr_control;
GRANT UPDATE(state,failed_attempts,consumed_principal,consumed_at_us)
    ON control.owner_challenges TO bigbrotr_control;
-- Row locks need UPDATE on at least one column, even on immutable records.
-- guard_producer_epoch() locks producer_slots during initial epoch INSERT.
GRANT UPDATE(producer_slot_id) ON control.producer_slots TO bigbrotr_control;
GRANT UPDATE(proof_id) ON control.reauthentication_proofs TO bigbrotr_control;
GRANT UPDATE(principal_id) ON control.principal_grants TO bigbrotr_control;
GRANT UPDATE(artifact_id) ON control.artifacts TO bigbrotr_control;
GRANT DELETE ON control.browser_sessions,control.reauthentication_proofs
    TO bigbrotr_control;

-- Explicit invoker helper closure, including functions called by triggers.
-- No EXECUTE ON ALL FUNCTIONS and no explicit trigger-function grants.
-- Trigger functions run from the already-created migration-owned triggers;
-- their invoker SQL still requires the table/helper grants supplied here.
GRANT EXECUTE ON FUNCTION
    control.acquisition_command_digest(control.acquisition_jobs,control.management_commands),
    control.acquisition_progress_bytes(control.acquisition_progress),
    control.acquisition_scope_digest(control.acquisition_scope_commands,control.management_commands),
    control.acquisition_selector_candidate(bytea),
    control.command_u64_bytes(control.u64),
    control.export_command_digest(control.export_jobs,control.management_commands),
    control.ingest_lineage_activation(bytea,numeric),
    control.ingest_lineage_closure_digest(bytea,numeric,boolean),
    control.ingest_lineage_handoff_at(bytea,numeric),
    control.ingest_lineage_manifest(bytea),
    control.ingest_lineage_manifest_at(bytea,numeric),
    control.ingest_lineage_u64(numeric),
    control.ingest_lineage_uint(bytea,integer,integer),
    control.initial_empty_retirement_digest(control.initial_ingest_successions),
    control.lock_activated_initial_retirement(bytea),
    control.lock_archive_pins(bytea,bytea),
    control.lock_ingest_lineage(bytea,numeric),
    control.lock_initial_empty_retirement(bytea,bytea),
    control.producer_progress_digest(control.producer_checkpoints),
    control.relay_endpoint_candidate(bytea),
    control.relay_enrollment_digest(control.relay_enrollment_changes,control.management_commands),
    control.relay_registration_digest(control.relay_registration_commands,control.management_commands),
    control.relay_route_valid(bytea),
    control.require_acquisition_execution(bytea,bytea,bytea,bytea,bytea),
    control.require_acquisition_job_authority(bytea,bytea),
    control.require_acquisition_scope(bytea,bytea,bytea,bytea),
    control.require_activated_initial_recovery(bytea),
    control.require_activated_initial_retirement(bytea),
    control.require_activated_initial_retirement_transition(bytea,bytea,bigint),
    control.require_activated_initial_source(bytea),
    control.require_active_initial_writer(bytea),
    control.require_archive_frontier_owner(bytea,bytea,numeric,bytea),
    control.require_archive_pin_holder(control.snapshot_leases,boolean),
    control.require_archive_pin_recovery(control.snapshot_leases),
    control.require_control_owner(bytea,bytea,numeric,bytea),
    control.require_empty_ingest_claims(bytea,numeric),
    control.require_empty_initial_retirement(bytea,bytea),
    control.require_export_execution_authority(bytea),
    control.require_ingest_admission(bytea,numeric,bytea,bytea,bytea,numeric,bytea),
    control.require_ingest_lineage_workflows(bytea,numeric,numeric),
    control.require_ingest_retirement_transition(bytea,numeric,bytea,bigint),
    control.require_ingest_successor(bytea,numeric,text),
    control.require_initial_empty_claims(control.initial_ingest_successions),
    control.require_initial_empty_transition(bytea,bytea,bigint),
    control.require_initial_retirement(bytea,numeric,bytea),
    control.require_prepared_initial_successor(bytea),
    control.require_producer_progress_capacity(bytea,numeric,bigint,integer,integer),
    control.require_relay_source(bytea,bytea,bytea,numeric,bytea),
    control.require_retained_producer_progress(bytea,numeric),
    control.retain_control_lock(),
    control.scope_pin_live(bytea,bytea,numeric,bytea)
    TO bigbrotr_control;
\endif

\if :reader
\connect bigbrotr_catalog
GRANT CONNECT ON DATABASE bigbrotr_catalog TO bigbrotr_archive_read;
GRANT USAGE ON SCHEMA public, bigbrotr_archive TO bigbrotr_archive_read;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO bigbrotr_archive_read;
GRANT SELECT ON bigbrotr_archive.installation, bigbrotr_archive.scientific_schema
    TO bigbrotr_archive_read;
\endif
