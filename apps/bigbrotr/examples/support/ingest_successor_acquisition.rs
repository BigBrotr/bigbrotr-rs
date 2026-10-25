//! Genuine typed acquisition intents and a distinct, fresh source-proof
//! abandonment consumer. Neither mode dispatches acquisition work or retires.
use super::*;
use bigbrotr::{
    acquisition::job::{AcquisitionCommand, JobDuration},
    management::{
        credentials::HASH_RESERVATION, grants::Revision, jobs::JobState, sessions::SessionLifetime,
    },
    storage::postgres::control_owner::ResolvedActivatedSuccessorSource,
};
use bigbrotr_model::AcquisitionScopeId;
use std::io::Read;

const JOB_BYTES: u64 = 128 * 1024;
const SCOPE: &str = "a39723d7c49af2c99498fb2f59c03b16";
const COMPACTED_SCOPE: &str = "422b7d8b33d4b55977806fb711e2d082";
const COMPACTED_SLOT: &str = "36a0253e6d94c18099f6c4dae9b7e040";
const REQUIRED_LATER_INPUT: &str =
    "36a0253e6d94c18099f6c4dae9b7e040-0000000000000002-0000000000000001.cbor";

pub(super) fn preflight(budget: &ResourceBudget, claims: bool) {
    // The grant overlaps the retained lineage/native and finite observations.
    // It is reacquired only after replay grants have ended, never as a vector.
    let bytes = if claims { HASH_RESERVATION } else { JOB_BYTES };
    drop(grant(budget, bytes, 0));
    emit(serde_json::json!({"event":"lineage-acquisition-resources",
        "purpose":if claims{"claims"}else{"abandon"},"data_bytes":bytes,
        "native_bytes":0,"max_jobs":if claims{2}else{64},"admission":false}));
}

impl Probe {
    pub(super) async fn abandon_successor_acquisition(
        &self,
        owner: &mut ControlSession,
        native: &mut Native,
        metadata: OwnedPermit,
        closing: OwnedPermit,
    ) -> Result<serde_json::Value, Failure> {
        // Publication spends this same opaque owner. This is a new inspection
        // and complete replay of the already closed source, never a rearm.
        self.source_pass(
            owner,
            native,
            metadata,
            closing,
            source::SourceFinish::Abandon,
        )
        .await
    }

    pub(super) async fn finish_acquisition(
        &self,
        owner: &mut ControlSession,
        proof: ResolvedActivatedSuccessorSource<'_>,
        epoch: u64,
    ) -> Result<u64, Failure> {
        self.snapshot("before-acquisition-abandon").await;
        let result = self
            .bounded(
                "acquisition-abandon",
                owner.abandon_activated_successor_acquisition(
                    proof,
                    self.request(),
                    grant(&self.budget, JOB_BYTES, 0),
                    |d, h| self.validate(d, h),
                ),
            )
            .await;
        self.snapshot("acquisition-abandon-returned").await;
        emit(
            serde_json::json!({"event":"lineage-acquisition-abandoned","epoch":epoch,
            "jobs":result.as_ref().ok(),"error":result.as_ref().err().map(|e|format!("{e:?}"))}),
        );
        result.map_err(|error| Failure::at("abandon", Some(epoch), None, error))
    }

    pub(super) async fn acquisition_claims(
        &self,
        owner: &mut ControlSession,
        native: &mut Native,
        metadata: OwnedPermit,
        mutation: OwnedPermit,
        compacted: bool,
    ) -> Result<serde_json::Value, Failure> {
        let target = if compacted {
            self.activate_live_in(owner, native, metadata, mutation, true)
                .await?
        } else {
            self.activate_live(owner, native, metadata, mutation)
                .await?
        };
        assert_eq!(target, 3, "finite terminal2 seed");
        let before_native = raw_state(&self.path);
        let before_inputs = originals(&self.path, compacted);
        self.snapshot("before-acquisition-claims").await;
        let result = self.claim_intents(owner, target, compacted).await;
        // Retain the actual final effects even on a failed typed operation.
        self.snapshot("acquisition-claims-returned").await;
        let after_native = raw_state(&self.path);
        let after_inputs = originals(&self.path, compacted);
        let unchanged = before_native == after_native && before_inputs == after_inputs;
        emit(
            serde_json::json!({"event":"lineage-acquisition-seed-state","target_epoch":target,
            "native_rows":after_native.0.len(),"native_sequence":after_native.1,
            "inputs_before":before_inputs,"inputs_after":after_inputs,"unchanged":unchanged,
            "executions":0,"error":result.as_ref().err().map(|e|&e.error)}),
        );
        if !unchanged {
            return Err(Failure::at(
                "claims-preservation",
                Some(target),
                None,
                OwnerError::InvalidMetadata,
            ));
        }
        result
    }

    async fn claim_intents(
        &self,
        owner: &mut ControlSession,
        target: u64,
        compacted: bool,
    ) -> Result<serde_json::Value, Failure> {
        let scope: AcquisitionScopeId = if compacted { COMPACTED_SCOPE } else { SCOPE }
            .parse()
            .unwrap();
        let lifetime = SessionLifetime::default();
        emit(
            serde_json::json!({"event":"lineage-acquisition-seed-plan","target_epoch":target,
            "scope":scope,"claimed_key":hex(&[0xa6;16]),"unclaimed_key":hex(&[0xa7;16]),
            "duration_us":30000000,"config_revision":1,"policy_revision":1,
            "username":"alice","peer":"127.0.0.1",
            "session_maximum_us":lifetime.maximum().as_micros().to_string(),
            "session_idle_us":lifetime.idle().as_micros().to_string(),"executions":0}),
        );
        let login = self
            .bounded(
                "claims-login-begin",
                owner.begin_browser_login("alice", "127.0.0.1".parse().unwrap()),
            )
            .await;
        self.snapshot("acquisition-login-begin-returned").await;
        emit(
            serde_json::json!({"event":"lineage-acquisition-login","phase":"begin",
            "error":login.as_ref().err().map(|e|format!("{e:?}"))}),
        );
        let login = login.map_err(|e| Failure::at("login-begin", Some(target), None, e))?;
        // This process is the finite blocking worker. Its hash permit is held
        // until synchronous verification and wiping finish; no detached task.
        let verified = login.verify(
            b"Independent owner password 2026",
            &mut grant(&self.budget, HASH_RESERVATION, 0),
        );
        self.snapshot("acquisition-login-verify-returned").await;
        emit(
            serde_json::json!({"event":"lineage-acquisition-login","phase":"verify",
            "error":verified.as_ref().err().map(|e|format!("{e:?}"))}),
        );
        let verified = verified.map_err(|e| Failure::at("login-verify", Some(target), None, e))?;
        let browser = self
            .bounded(
                "claims-login-finish",
                owner.finish_browser_login(verified, lifetime),
            )
            .await;
        self.snapshot("acquisition-login-finish-returned").await;
        emit(
            serde_json::json!({"event":"lineage-acquisition-login","phase":"finish",
            "error":browser.as_ref().err().map(|e|format!("{e:?}"))}),
        );
        let browser = browser.map_err(|e| Failure::at("login-finish", Some(target), None, e))?;
        let refreshed = self
            .bounded("claims-refresh", owner.refresh(|d, h| self.validate(d, h)))
            .await;
        self.snapshot("acquisition-refresh-returned").await;
        emit(serde_json::json!({"event":"lineage-acquisition-refresh",
            "policy":refreshed.as_ref().ok().map(|p|serde_json::json!({"config_revision":p.config_revision,
                "policy_revision":p.policy_revision,"config_digest":hex(&p.config_digest),"policy_digest":hex(&p.policy_digest)})),
            "error":refreshed.as_ref().err().map(|e|format!("{e:?}"))}));
        let policy = refreshed.map_err(|e| Failure::at("claims-refresh", Some(target), None, e))?;
        assert_eq!((policy.config_revision, policy.policy_revision), (1, 1));
        let command = AcquisitionCommand {
            scope_id: scope,
            deadline_us: JobDuration::new(30_000_000).unwrap(),
            expected_config_revision: Revision::try_from("1".to_owned()).unwrap(),
            expected_policy_revision: Revision::try_from("1".to_owned()).unwrap(),
        };
        let started = Cell::new(false);
        let accepted = self
            .bounded(
                "claims-accept",
                owner.accept_acquisition(
                    &browser.session,
                    &browser.csrf,
                    [0xa6; 16].try_into().unwrap(),
                    &command,
                    &started,
                ),
            )
            .await;
        self.snapshot("acquisition-claimed-accept-returned").await;
        emit(
            serde_json::json!({"event":"lineage-acquisition-acceptance","purpose":"claimed",
            "request_key":hex(&[0xa6;16]),"receipt":accepted.as_ref().ok(),
            "error":accepted.as_ref().err().map(|e|format!("{e:?}")),"commit_started":started.get()}),
        );
        let accepted = accepted.map_err(|e| Failure::at("claims-accept", Some(target), None, e))?;
        assert!(started.get() && accepted.created);
        let started = Cell::new(false);
        let claimed = self
            .bounded(
                "claims-claim",
                owner.claim_acquisition(accepted.job_id, &started),
            )
            .await;
        self.snapshot("acquisition-claim-returned").await;
        emit(serde_json::json!({"event":"lineage-acquisition-claim",
            "job":claimed.as_ref().ok().map(|j|serde_json::json!({"id":j.id,"scope":j.scope,
                "state":j.state,"job_revision":j.revision,"checkpoint_revision":j.checkpoint_revision,
                "accepted_at_us":j.accepted_at_us,"deadline_at_us":j.deadline_at_us,
                "owner":j.owner.map(|i|serde_json::json!({"resource":i.resource,"epoch":i.epoch.get(),"incarnation":i.incarnation}))})),
            "error":claimed.as_ref().err().map(|e|format!("{e:?}")),"commit_started":started.get()}));
        let claimed = claimed.map_err(|e| Failure::at("claims-claim", Some(target), None, e))?;
        assert!(started.get());
        assert_eq!(
            (
                claimed.id,
                claimed.scope,
                claimed.state,
                claimed.revision,
                claimed.checkpoint_revision
            ),
            (accepted.job_id, scope, JobState::Queued, 2, 0)
        );
        assert!(
            claimed
                .owner
                .is_some_and(|id| id.resource == owner.identity().resource_id
                    && id.epoch == owner.identity().epoch
                    && id.incarnation == owner.identity().process_incarnation)
        );
        let started = Cell::new(false);
        let unclaimed = self
            .bounded(
                "claims-unclaimed",
                owner.accept_acquisition(
                    &browser.session,
                    &browser.csrf,
                    [0xa7; 16].try_into().unwrap(),
                    &command,
                    &started,
                ),
            )
            .await;
        self.snapshot("acquisition-unclaimed-accept-returned").await;
        emit(
            serde_json::json!({"event":"lineage-acquisition-acceptance","purpose":"unclaimed",
            "request_key":hex(&[0xa7;16]),"receipt":unclaimed.as_ref().ok(),
            "error":unclaimed.as_ref().err().map(|e|format!("{e:?}")),"commit_started":started.get()}),
        );
        let unclaimed =
            unclaimed.map_err(|e| Failure::at("claims-unclaimed", Some(target), None, e))?;
        assert!(started.get() && unclaimed.created);
        assert_ne!(unclaimed.job_id, claimed.id);
        Ok(
            serde_json::json!({"target_epoch":target,"claimed_job":claimed.id,"unclaimed_job":unclaimed.job_id,
            "requests":0,"source_cut":0,"executions":0,"writer_delivered":true,
            "session_revoked":false,"workflow":false,"source_closure":false,"retirement":false}),
        )
    }
}

fn originals(path: &Path, compacted: bool) -> Vec<serde_json::Value> {
    let maximum = if compacted { 8 } else { 4 };
    let mut result: Vec<_> = std::fs::read_dir(path.join("producer-inputs-v1"))
        .unwrap()
        .take(maximum + 1)
        .map(|entry| {
            let entry = entry.unwrap();
            assert!(entry.file_type().unwrap().is_file());
            let name = entry.file_name().into_string().unwrap();
            if compacted {
                // Fixed Terminal2 vocabulary: every present residual is
                // observed, and none can substitute for the required2:1 input.
                assert!(
                    name.strip_prefix(COMPACTED_SLOT)
                        .is_some_and(|suffix| matches!(
                            suffix,
                            "-0000000000000001-0000000000000001.cbor"
                                | "-0000000000000001-0000000000000002.cbor"
                                | "-0000000000000001-0000000000000003.cbor"
                                | "-0000000000000001-0000000000000004.cbor"
                                | "-0000000000000001-0000000000000005.cbor"
                                | "-0000000000000001-0000000000000006.cbor"
                                | "-0000000000000001-0000000000000007.cbor"
                                | "-0000000000000002-0000000000000001.cbor"
                        ))
                );
            }
            let mut bytes = Vec::new();
            std::fs::File::open(entry.path())
                .unwrap()
                .take(8193)
                .read_to_end(&mut bytes)
                .unwrap();
            assert!(bytes.len() <= 8192);
            serde_json::json!({"name":name,"bytes":hex(&bytes)})
        })
        .collect();
    if compacted {
        assert!((1..=maximum).contains(&result.len()));
        assert!(
            result
                .iter()
                .any(|input| input["name"].as_str() == Some(REQUIRED_LATER_INPUT))
        );
    } else {
        assert_eq!(
            result.len(),
            4,
            "four retained predecessor inputs, no target input"
        );
    }
    result.sort_unstable_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
    result
}
