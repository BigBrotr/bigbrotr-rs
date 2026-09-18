-- Synthetic coordination-only prerequisite; NEVER a product bootstrap policy.
-- Run only in a freshly migrated disposable registry lab database.
BEGIN;
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM control.datasets) THEN
        RAISE EXCEPTION 'registry fixture requires an empty application database';
    END IF;
END $$;
INSERT INTO control.datasets(dataset_id,instance_id,created_at_us)
VALUES (decode(repeat('01',16),'hex'),decode(repeat('02',16),'hex'),0);
INSERT INTO control.config_revisions(revision,dataset_id,schema_version,authority_mode,document,document_digest,accepted_source_digest,accepted_at_us,actor_id)
VALUES (1,decode(repeat('01',16),'hex'),1,'managed','{"fixture":"registry-coordination-only"}',decode(repeat('08',32),'hex'),decode(repeat('18',32),'hex'),0,decode(repeat('10',16),'hex'));
INSERT INTO control.policy_revisions(revision,dataset_id,config_revision,schema_version,document,document_digest,accepted_at_us,actor_id)
VALUES (1,decode(repeat('01',16),'hex'),1,1,'{"fixture":"registry-coordination-only"}',decode(repeat('09',32),'hex'),0,decode(repeat('10',16),'hex'));
UPDATE control.datasets SET config_revision=1,policy_revision=1;
INSERT INTO control.role_instances(role_instance_id,dataset_id,role,created_at_us)
VALUES (decode(repeat('04',16),'hex'),decode(repeat('01',16),'hex'),'control',0);
INSERT INTO control.owner_epochs(resource_id,epoch,dataset_id,role,role_instance_id,process_incarnation,acquired_at_us,lease_expires_at_us,state)
VALUES (decode(repeat('03',16),'hex'),1,decode(repeat('01',16),'hex'),'control',decode(repeat('04',16),'hex'),decode(repeat('05',16),'hex'),0,60000000,'active');
-- Exact application mutation boundary exercised here; no DDL, owner role,
-- DELETE/TRUNCATE or migration ledger INSERT/UPDATE privilege.
GRANT USAGE ON SCHEMA control TO bigbrotr_control;
GRANT SELECT ON ALL TABLES IN SCHEMA control TO bigbrotr_control;
GRANT INSERT ON control.role_instances,control.owner_epochs,control.topology_generations,control.source_streams,control.topology_streams,control.allocation_domains,control.producer_slots,control.producer_epochs TO bigbrotr_control;
GRANT UPDATE(topology_generation) ON control.datasets TO bigbrotr_control;
GRANT UPDATE(sealed) ON control.topology_generations TO bigbrotr_control;
GRANT UPDATE(retired_at_us) ON control.role_instances TO bigbrotr_control;
GRANT UPDATE(state) ON control.owner_epochs,control.source_streams,control.allocation_domains,control.producer_epochs TO bigbrotr_control;
-- PostgreSQL requires an UPDATE privilege for the trigger's FOR UPDATE lock;
-- the immutable slot trigger still refuses actual updates to this column.
GRANT UPDATE(producer_slot_id) ON control.producer_slots TO bigbrotr_control;
COMMIT;
