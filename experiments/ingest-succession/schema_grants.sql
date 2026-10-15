-- Additional application privileges for the isolated0038 SQL qualification.
-- Apply only after0038, retaining the existing initial/activated recovery grants.
-- This grants no native custody or writer capability. Destructive negative-case
-- privileges belong to the disposable fixture clone, not this application set.
GRANT SELECT ON control.ingest_successions,control.ingest_activations,
    control.ingest_terminal_transactions TO bigbrotr_control;
-- Terminal insertion stays in the actual backend's privilege context. Its
-- BEFORE guard derives Control/xid and requires the already-retired producer;
-- immutable history and the primary key prevent substitution or rewriting.
GRANT INSERT ON control.ingest_successions,control.ingest_activations,
    control.ingest_terminal_transactions TO bigbrotr_control;
GRANT EXECUTE ON FUNCTION
    control.ingest_lineage_uint(bytea,integer,integer),
    control.ingest_lineage_u64(numeric),
    control.ingest_lineage_manifest(bytea),
    control.ingest_lineage_manifest_at(bytea,numeric),
    control.ingest_lineage_handoff_at(bytea,numeric),
    control.ingest_lineage_activation(bytea,numeric),
    control.ingest_lineage_closure_digest(bytea,numeric,boolean),
    control.require_ingest_lineage_workflows(bytea,numeric,numeric),
    control.lock_ingest_lineage(bytea,numeric),
    control.require_ingest_successor(bytea,numeric,text),
    control.require_empty_ingest_claims(bytea,numeric),
    control.require_ingest_retirement_transition(bytea,numeric,bytea,bigint),
    control.require_ingest_admission(bytea,numeric,bytea,bytea,bytea,numeric,bytea)
    TO bigbrotr_control;
