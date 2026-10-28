//! Bounded operator observations of real pre-effect capacity waits.
//! These records neither authorize effects nor prove a remote query outcome.
use super::{
    Progress,
    admission::{Refusal, Role},
};
use bigbrotr::{
    runtime::{BudgetSnapshot, ResourceLimits},
    storage::postgres::ingest_registry::ControlOwner,
};
use bigbrotr_model::{DatasetId, ProcessIncarnation};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use tokio::{sync::watch, time::Instant};

const RECORDS: u16 = 128;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct RetainedIndex {
    pub incarnation: ProcessIncarnation,
    pub sequence: u64,
    pub kind: &'static str,
}
#[derive(Clone, Copy, Default)]
pub(super) struct IndexObservation {
    pub retained: Option<RetainedIndex>,
    pub local_joined_consumed_through: u64,
    pub remote_unknown: bool,
}
#[derive(Clone, Copy)]
struct Wait {
    id: u64,
    started: Instant,
    role: Role,
    refusal: Refusal,
    index: IndexObservation,
}
#[derive(Clone, Copy)]
struct Record {
    wait: Wait,
    captured: Instant,
    admitted: bool,
    budget: BudgetSnapshot,
    index: IndexObservation,
}

struct Pending {
    records: [Option<Record>; 2],
    emitted: u16,
    disabled: bool,
    incomplete: bool,
}
pub(super) struct Mailbox {
    dataset: DatasetId,
    owner: ControlOwner,
    origin: Instant,
    pending: Mutex<Pending>,
}
impl Mailbox {
    fn push(&self, record: Record) {
        let mut pending = self.pending.lock().expect("capacity mailbox");
        if pending.disabled {
            return;
        }
        if pending.emitted == RECORDS || pending.records[1].is_some() {
            // Preserve both original records. Mark subsequent evidence incomplete
            // once, without an unbounded queue or a fabricated replacement pair.
            pending.disabled = true;
            pending.incomplete = true;
            return;
        }
        let slot = usize::from(pending.records[0].is_some());
        pending.records[slot] = Some(record);
        pending.emitted += 1;
    }
    pub(super) fn drain(&self, emit: &mut impl FnMut(Value)) {
        let (records, incomplete) = {
            let mut pending = self.pending.lock().expect("capacity mailbox");
            (
                std::mem::replace(&mut pending.records, [None, None]),
                std::mem::take(&mut pending.incomplete),
            )
        };
        for record in records.into_iter().flatten() {
            emit(self.report(Some(record)));
        }
        if incomplete {
            emit(self.report(None));
        }
    }
    pub(super) fn has_pending(&self) -> bool {
        let pending = self.pending.lock().expect("capacity mailbox");
        pending.records[0].is_some() || pending.incomplete
    }
    fn report(&self, record: Option<Record>) -> Value {
        let mut value = json!({
            "format":"bigbrotr-initial-capacity/1", "scope":"operator-observation-only",
            "dataset_id":self.dataset, "control_resource_id":self.owner.resource_id,
            "control_epoch":self.owner.epoch.get().to_string(),
            "control_incarnation":self.owner.process_incarnation,
            "control_role_instance_id":self.owner.role_instance_id.iter().map(|b| format!("{b:02x}")).collect::<String>(),
            "transition":"incomplete", "record_limit":RECORDS,
        });
        if let Some(record) = record {
            let wait = record.wait;
            let released = wait.index.retained.is_some_and(|held| {
                held.incarnation == self.owner.process_incarnation
                    && record.index.local_joined_consumed_through >= held.sequence
            });
            value["transition"] = json!(if record.admitted {
                "admitted"
            } else {
                "deferred"
            });
            value["wait_id"] = json!(wait.id.to_string());
            value["role"] = json!(match wait.role {
                Role::Archive => "archive",
                Role::Sync => "sync",
            });
            value["timing"] = json!("monotonic-microseconds-from-capacity-observer-start");
            value["wait_started_us"] = json!(micros(wait.started.duration_since(self.origin)));
            value["observed_us"] = json!(micros(record.captured.duration_since(self.origin)));
            value["wait_elapsed_us"] = json!(micros(record.captured.duration_since(wait.started)));
            value["budget_domain"] = json!(wait.refusal.domain);
            value["budget_observation"] = json!(if record.admitted {
                "after-whole-operation-reservation"
            } else {
                "after-atomic-refusal-not-the-failing-snapshot"
            });
            value["requested"] = units(wait.refusal.requested);
            value["charged"] = units(record.budget.used());
            value["limits"] = units(record.budget.limits());
            value["retained_index_at_denial"] = retained(wait.index.retained);
            value["retained_index_now"] = retained(record.index.retained);
            value["local_joined_consumed_through"] =
                json!(record.index.local_joined_consumed_through.to_string());
            value["denied_index_locally_joined_and_consumed"] = json!(released);
            value["remote_query_unknown"] = json!(record.index.remote_unknown);
        }
        value
    }
}
fn micros(value: std::time::Duration) -> u64 {
    u64::try_from(value.as_micros()).unwrap_or(u64::MAX)
}
fn units(value: ResourceLimits) -> Value {
    json!({"data_bytes":value.data_bytes(), "items":value.items(),
        "native_bytes":value.native_bytes(), "control_bytes":value.control_bytes()})
}
fn retained(value: Option<RetainedIndex>) -> Value {
    value.map_or(Value::Null, |v| {
        json!({"incarnation":v.incarnation,
        "sequence":v.sequence.to_string(), "kind":v.kind,
        "state":"actual-retained-operation-owner"})
    })
}

pub(super) struct Trace<'a> {
    progress: &'a watch::Sender<Progress>,
    mailbox: Arc<Mailbox>,
    next: u64,
    archive: Option<Wait>,
    sync: Option<Wait>,
}
impl<'a> Trace<'a> {
    pub(super) fn new(
        dataset: DatasetId,
        owner: ControlOwner,
        progress: &'a watch::Sender<Progress>,
    ) -> Self {
        let mailbox = Arc::new(Mailbox {
            dataset,
            owner,
            origin: Instant::now(),
            pending: Mutex::new(Pending {
                records: [None, None],
                emitted: 0,
                disabled: false,
                incomplete: false,
            }),
        });
        progress.send_modify(|value| value.capacity = Some(mailbox.clone()));
        Self {
            progress,
            mailbox,
            next: 1,
            archive: None,
            sync: None,
        }
    }
    fn slot(&mut self, role: Role) -> &mut Option<Wait> {
        match role {
            Role::Archive => &mut self.archive,
            Role::Sync => &mut self.sync,
        }
    }
    pub(super) fn denied(
        &mut self,
        role: Role,
        refusal: Refusal,
        index: IndexObservation,
        now: Instant,
    ) -> super::Result<()> {
        let wait = Wait {
            id: self.next,
            started: now,
            role,
            refusal,
            index,
        };
        self.next = self
            .next
            .checked_add(1)
            .ok_or_else(|| (4, "capacity wait identity exhausted".into()))?;
        assert!(
            self.slot(role).replace(wait).is_none(),
            "one observation per wait"
        );
        self.mailbox.push(Record {
            wait,
            captured: now,
            admitted: false,
            budget: refusal.observed,
            index,
        });
        self.progress.send_modify(|_| {});
        Ok(())
    }
    pub(super) fn admitted(
        &mut self,
        role: Role,
        application: BudgetSnapshot,
        native: Option<BudgetSnapshot>,
        index: IndexObservation,
        now: Instant,
    ) {
        if let Some(wait) = self.slot(role).take() {
            let budget = match wait.refusal.domain {
                "application" => application,
                "native" => native.expect("Archive native admission snapshot"),
                _ => unreachable!("closed budget domains"),
            };
            self.mailbox.push(Record {
                wait,
                captured: now,
                admitted: true,
                budget,
                index,
            });
            self.progress.send_modify(|_| {});
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::{
        Cancellation,
        admission::{self, Admission},
        supervision,
    };
    use super::*;
    use bigbrotr::runtime::{ProcessEnvelope, ResourceBudget, ResourceRequest, WaiterLimits};
    use std::time::Duration;

    fn progress() -> (watch::Sender<Progress>, watch::Receiver<Progress>) {
        watch::channel(Progress {
            deadline: Instant::now() + Duration::from_secs(5),
            phase: None,
            initialized: None,
            shutdown: None,
            sync: None,
            capacity: None,
        })
    }
    fn owner() -> ControlOwner {
        ControlOwner {
            resource_id: [2; 16].try_into().unwrap(),
            epoch: bigbrotr_model::OwnerEpoch::new(1).unwrap(),
            role_instance_id: [3; 16],
            process_incarnation: [4; 16].try_into().unwrap(),
        }
    }
    fn refused() -> Refusal {
        let budget = ResourceBudget::new(
            ProcessEnvelope::new(
                100,
                0,
                ResourceLimits::new(100, 4, 0, 0),
                WaiterLimits::new(0, 0),
            )
            .unwrap(),
        );
        let _held = budget
            .try_acquire(ResourceRequest::data(70, 1, 0).unwrap())
            .unwrap();
        let Admission::Deferred(refusal) =
            admission::reserve(&budget, ResourceLimits::new(60, 1, 0, 0), "application").unwrap()
        else {
            panic!("actual refusal")
        };
        refusal
    }
    fn held() -> IndexObservation {
        IndexObservation {
            retained: Some(RetainedIndex {
                incarnation: owner().process_incarnation,
                sequence: 7,
                kind: "publication",
            }),
            local_joined_consumed_through: 6,
            remote_unknown: false,
        }
    }

    #[test]
    fn denial_and_admission_survive_coalesced_watch_with_exact_wait_and_join_identity() {
        let (progress, mut observed) = progress();
        let mut trace = Trace::new([1; 16].try_into().unwrap(), owner(), &progress);
        let refusal = refused();
        trace
            .denied(Role::Sync, refusal, held(), Instant::now())
            .unwrap();
        trace.admitted(
            Role::Sync,
            refusal.observed,
            None,
            IndexObservation {
                retained: None,
                local_joined_consumed_through: 7,
                remote_unknown: false,
            },
            Instant::now(),
        );
        observed.borrow_and_update(); // One watch observation cannot erase either record.
        let mut records = Vec::new();
        trace.mailbox.drain(&mut |record| records.push(record));
        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["transition"], "deferred");
        assert_eq!(records[1]["transition"], "admitted");
        assert_eq!(records[0]["wait_id"], records[1]["wait_id"]);
        assert_eq!(
            records[0]["retained_index_at_denial"],
            records[1]["retained_index_at_denial"]
        );
        assert_eq!(
            records[0]["denied_index_locally_joined_and_consumed"],
            false
        );
        assert_eq!(records[1]["denied_index_locally_joined_and_consumed"], true);
        assert_eq!(
            records[0]["budget_observation"],
            "after-atomic-refusal-not-the-failing-snapshot"
        );
        for record in records {
            let line = serde_json::to_string(&record).unwrap();
            assert!(line.len() + 1 <= 4096);
            assert!(
                !line.contains("query_id")
                    && !line.contains("password")
                    && !line.contains("endpoint")
            );
        }
        assert!(!trace.mailbox.has_pending());
    }

    #[test]
    fn overflow_preserves_original_pair_and_marks_incomplete_without_reconstruction() {
        let (progress, _) = progress();
        let mut trace = Trace::new([1; 16].try_into().unwrap(), owner(), &progress);
        let refusal = refused();
        trace
            .denied(Role::Archive, refusal, held(), Instant::now())
            .unwrap();
        trace.admitted(
            Role::Archive,
            refusal.observed,
            None,
            held(),
            Instant::now(),
        );
        trace
            .denied(Role::Sync, refusal, held(), Instant::now())
            .unwrap();
        let mut records = Vec::new();
        trace.mailbox.drain(&mut |record| records.push(record));
        assert_eq!(records.len(), 3);
        assert_eq!(records[0]["wait_id"], "1");
        assert_eq!(records[1]["wait_id"], "1");
        assert_eq!(
            records[1]["denied_index_locally_joined_and_consumed"],
            false
        );
        assert_eq!(records[2]["transition"], "incomplete");
        trace.admitted(Role::Sync, refusal.observed, None, held(), Instant::now());
        assert!(!trace.mailbox.has_pending());
        assert!(!std::mem::needs_drop::<Record>());
        assert!(std::mem::size_of::<Pending>() <= 2048);
    }

    #[test]
    fn record_cap_and_native_domain_remain_explicit_without_remote_exclusion() {
        let (progress, _) = progress();
        let mut trace = Trace::new([1; 16].try_into().unwrap(), owner(), &progress);
        let mut refusal = refused();
        refusal.domain = "native";
        let mut count = 0;
        for _ in 0..RECORDS / 2 {
            trace
                .denied(Role::Archive, refusal, held(), Instant::now())
                .unwrap();
            trace.admitted(
                Role::Archive,
                refusal.observed,
                Some(refusal.observed),
                IndexObservation {
                    retained: None,
                    local_joined_consumed_through: 7,
                    remote_unknown: true,
                },
                Instant::now(),
            );
            trace.mailbox.drain(&mut |record| {
                assert_eq!(record["budget_domain"], "native");
                if record["transition"] == "admitted" {
                    assert_eq!(record["remote_query_unknown"], true);
                }
                count += 1;
            });
        }
        assert_eq!(count, RECORDS);
        trace
            .denied(Role::Archive, refusal, held(), Instant::now())
            .unwrap();
        let mut records = Vec::new();
        trace.mailbox.drain(&mut |record| records.push(record));
        assert_eq!(records.len(), 1);
        assert_eq!(records[0]["transition"], "incomplete");
    }

    #[tokio::test]
    async fn terminal_and_stop_grace_join_drain_capacity_without_extending_deadline() {
        for stopping in [false, true] {
            let (progress, mut observed) = progress();
            let deadline = observed.borrow().deadline;
            let cancellation = Arc::new(Cancellation::new());
            let worker_cancel = cancellation.clone();
            let mut worker = tokio::spawn(async move {
                if stopping {
                    worker_cancel.wait().await;
                }
                let mut trace = Trace::new([1; 16].try_into().unwrap(), owner(), &progress);
                trace.denied(Role::Sync, refused(), held(), Instant::now())?;
                Ok(json!({"state":"terminal"}))
            });
            if !stopping {
                while !worker.is_finished() {
                    tokio::task::yield_now().await;
                }
            }
            let mut records = Vec::new();
            let joined = supervision::collect(
                &mut worker,
                &mut observed,
                &cancellation,
                async {
                    if !stopping {
                        std::future::pending::<()>().await;
                    }
                },
                Duration::from_secs(1),
                |record| records.push(record),
            )
            .await;
            assert!(matches!(
                joined,
                supervision::Collected::Joined {
                    terminal: Ok(Ok(_)),
                    interrupted: false
                }
            ));
            assert_eq!(observed.borrow().deadline, deadline);
            assert_eq!(records.len(), 1);
            assert_eq!(records[0]["transition"], "deferred");
        }
    }
}
