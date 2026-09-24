//! Private browser transport; domain operations remain owned by Control.
mod assets;
mod boundary;
mod delivery;
mod history;
pub(super) use history::{HistoryBody, HistoryWork};
mod transport;
pub(super) use assets::Assets;
use bigbrotr::runtime::policy_authority::{DeliveryWindow, PolicyLeaseTracker};
use bigbrotr::{
    export::destination::{CheckedDestination, LocalDestination},
    management::{
        credentials::HASH_RESERVATION,
        grants::{AllocationSelection, GrantCommand},
        jobs::{ExportCommand, JobCommand},
        reauthentication::VerifiedReauthentication,
        sessions::{BrowserSecret, SessionError, SessionLifetime, VerifiedLogin},
    },
    runtime::{ProcessEnvelope, ResourceBudget, ResourceLimits, ResourceRequest, WaiterLimits},
    storage::postgres::control_owner::{ControlSession, ScopeGrantAllocation},
};
use bigbrotr_model::{CommandKey, QualifiedRef, ReauthenticationId};
use boundary::BrowserBoundary;
use delivery::DeliveryGate;
use std::{future::Future, net::SocketAddr, sync::Arc, time::Duration};
use tokio::task::JoinSet;
use transport::{Command, Completion, Operation, Reply, Server};

type Result<T> = std::result::Result<T, (u8, String)>;
enum VerifiedPassword {
    Login(VerifiedLogin),
    Reauthentication(VerifiedReauthentication),
}
type HashResult = (
    Completion,
    std::result::Result<VerifiedPassword, SessionError>,
);

pub(super) struct Work(WorkKind);
enum WorkKind {
    Command(Command),
    Hash(HashResult),
    Failed,
}
/// Held until the supervisor rechecks its suspend-aware authority clock. A
/// completed database operation alone does not authorize browser delivery.
pub(super) struct Response(Completion, std::result::Result<Reply, SessionError>);
impl Response {
    pub fn deliver(self) {
        if !self.0.abandoned() {
            self.0.finish(self.1);
        }
    }
}
/// Control authenticated new work and rolled back its first transaction. Both
/// consumers require the same native allocation before full atomic acceptance.
pub(super) struct AllocationWork {
    completion: Completion,
    session: BrowserSecret,
    csrf: BrowserSecret,
    key: CommandKey,
    kind: AllocationKind,
}
enum AllocationKind {
    Grant {
        command: GrantCommand,
        proof: ReauthenticationId,
    },
    Export {
        command: ExportCommand,
        destination: Arc<LocalDestination>,
    },
}
impl AllocationWork {
    pub fn allocation(&self) -> Option<&AllocationSelection> {
        match &self.kind {
            AllocationKind::Grant { command, .. } => command.allocation.as_ref(),
            AllocationKind::Export { command, .. } => Some(&command.allocation),
        }
    }
    pub fn scope(&self) -> QualifiedRef {
        match &self.kind {
            AllocationKind::Grant { command, .. } => command.scope,
            AllocationKind::Export { command, .. } => command.query.scope,
        }
    }
    pub fn destination(&self) -> Option<Arc<LocalDestination>> {
        match &self.kind {
            AllocationKind::Export { destination, .. } => Some(destination.clone()),
            AllocationKind::Grant { .. } => None,
        }
    }
    pub fn deadline(&self) -> tokio::time::Instant {
        self.completion.deadline
    }
    pub fn abandoned(&self) -> bool {
        self.completion.abandoned()
    }
    pub fn refuse(self, error: SessionError) -> Response {
        Response(self.completion, Err(error))
    }
    pub async fn accept(
        self,
        owner: &mut ControlSession,
        maximum: Duration,
        allocation: &ScopeGrantAllocation<'_, '_>,
        destination: Option<&CheckedDestination>,
    ) -> Result<Response> {
        if self.abandoned() {
            return Ok(self.refuse(SessionError::ArchiveUnavailable));
        }
        let commit = std::cell::Cell::new(false);
        let result = match self.kind {
            AllocationKind::Grant { command, proof } => sql(
                maximum,
                owner.accept_scope_grant(
                    &self.session,
                    &self.csrf,
                    self.key,
                    &command,
                    proof,
                    Some(allocation),
                    &commit,
                ),
            )
            .await?
            .map(Reply::Grant),
            AllocationKind::Export { command, .. } => sql(
                maximum,
                owner.accept_export(
                    &self.session,
                    &self.csrf,
                    self.key,
                    &command,
                    Some(allocation),
                    destination,
                    &commit,
                ),
            )
            .await?
            .map(Reply::Job),
        };
        Ok(Response(self.completion, result))
    }
}
pub(super) enum Execution {
    Complete(Option<Response>),
    Archive(AllocationWork),
    History(HistoryWork),
}

pub(super) struct Management {
    server: Server,
    hashes: JoinSet<HashResult>,
    budget: ResourceBudget,
    delivery: DeliveryGate,
    export_jobs: bool,
}

pub(super) fn budget(root: &crate::config::Resolved) -> Result<ResourceBudget> {
    let n = |name: &str| root.number(&format!("runtime.processes.application.{name}"));
    // One Control, one hash, one optional asset bundle, and separate connection/
    // request ownership. Counts never supply an additional memory allowance.
    let items = 3 + transport::CONNECTIONS as u64 + transport::QUEUE as u64;
    if root.flag("archive.reader.enabled") {
        // Control, manager, secrets, native output, hash and HTTP all use this
        // same ledger; a second application payload allowance is never created.
        return super::archive_preflight::budget(root, items + 6);
    }
    let envelope = ProcessEnvelope::new(
        n("memory_max_bytes"),
        n("baseline_bytes"),
        ResourceLimits::new(
            n("payload_bytes"),
            items,
            n("native_bytes"),
            n("completion_reserve_bytes"),
        ),
        WaiterLimits::new(0, 0),
    )
    .map_err(|_| (3, "invalid private management process envelope".into()))?;
    Ok(ResourceBudget::new(envelope))
}

pub(super) fn prepare_assets(
    root: &crate::config::Resolved,
    budget: &ResourceBudget,
) -> Result<Option<Assets>> {
    root.text("administration.assets.directory")
        .map(|directory| {
            Assets::load(
                std::path::Path::new(directory),
                root.text("administration.assets.base_path")
                    .expect("registered asset mount"),
                budget,
            )
            .map_err(|error| (3, format!("private asset bundle refused: {error:?}")))
        })
        .transpose()
}

impl Management {
    pub async fn start(
        root: &crate::config::Resolved,
        budget: ResourceBudget,
        window: DeliveryWindow,
        assets: Option<Assets>,
        export_jobs: bool,
    ) -> Result<Self> {
        let address = SocketAddr::new(
            root.text("administration.listen.host")
                .ok_or_else(|| (3, "missing private listener host".into()))?
                .parse()
                .map_err(|_| (3, "invalid private listener address".into()))?,
            u16::try_from(root.number("administration.listen.port"))
                .map_err(|_| (3, "invalid private listener port".into()))?,
        );
        // Check that the declared envelope can support at least one complete
        // login alongside the already-owned Control transport before binding.
        let capacity = budget
            .try_acquire(
                ResourceRequest::data(
                    HASH_RESERVATION + transport::CONNECTION_BYTES + transport::REQUEST_BYTES,
                    3,
                    0,
                )
                .expect("fixed login reservation"),
            )
            .map_err(|_| {
                (
                    3,
                    "private management cannot reserve one complete login".into(),
                )
            })?;
        drop(capacity);
        let delivery = DeliveryGate::new(window);
        let server = Server::bind(
            address,
            budget.clone(),
            delivery.clone(),
            assets,
            transport::HistoryLimits::configured(root),
        )
        .await
        .map_err(|_| (1, "private management listener unavailable".into()))?;
        Ok(Self {
            server,
            hashes: JoinSet::new(),
            budget,
            delivery,
            export_jobs,
        })
    }

    /// Both receive and join_next are cancellation-safe when Control renewal or
    /// shutdown wins the surrounding supervisor select.
    pub async fn next(&mut self) -> Work {
        tokio::select! {
            biased;
            result = self.hashes.join_next(), if !self.hashes.is_empty() => match result {
                Some(Ok(result)) => Work(WorkKind::Hash(result)),
                _ => Work(WorkKind::Failed),
            },
            command = self.server.commands.recv() => Work(command.map(WorkKind::Command).unwrap_or(WorkKind::Failed)),
        }
    }

    pub async fn execute(
        &mut self,
        work: Work,
        owner: &mut ControlSession,
        lease: &mut PolicyLeaseTracker,
        maximum: Duration,
        native_reader: bool,
        destination: Option<&Arc<LocalDestination>>,
    ) -> Result<Execution> {
        match work.0 {
            WorkKind::Failed => {
                return Err((1, "private management worker stopped unexpectedly".into()));
            }
            WorkKind::Hash((completion, result)) => {
                if completion.abandoned() {
                    return Ok(Execution::Complete(None));
                }
                let result = match result {
                    Ok(VerifiedPassword::Login(verified)) => sql(
                        maximum,
                        owner.finish_browser_login(verified, SessionLifetime::default()),
                    )
                    .await
                    .map(|r| r.map(Reply::Login))?,
                    Ok(VerifiedPassword::Reauthentication(verified)) => {
                        sql(maximum, owner.finish_browser_reauthentication(verified))
                            .await
                            .map(|r| r.map(Reply::Reauthenticated))?
                    }
                    Err(e) => Err(e),
                };
                return Ok(Execution::Complete(Some(Response(completion, result))));
            }
            WorkKind::Command(Command {
                operation,
                completion,
            }) => {
                if completion.abandoned() {
                    return Ok(Execution::Complete(None));
                }
                match operation {
                    Operation::JobStatus {
                        session,
                        csrf,
                        job,
                        delivery,
                    } => {
                        if !self.export_jobs {
                            return Ok(Execution::Complete(Some(Response(
                                completion,
                                Err(SessionError::ArchiveUnavailable),
                            ))));
                        }
                        let action = lease
                            .begin_action(lease.known_epoch())
                            .map_err(|e| (4, e.to_string()))?;
                        let result =
                            sql(maximum, owner.observe_browser_job(&session, &csrf, job)).await?;
                        let result = match result {
                            Ok(status) => {
                                // Renewing the enclosing Control lease must not
                                // extend this session's authority at socket IO.
                                match action.complete(status.remaining()) {
                                    Ok(window) => match delivery.action.bind(window) {
                                        Ok(()) => Ok(Reply::JobStatus(status.into_observation())),
                                        Err(_) => Err(SessionError::Unavailable),
                                    },
                                    _ => Err(SessionError::Unavailable),
                                }
                            }
                            Err(error) => Err(error),
                        };
                        return Ok(Execution::Complete(Some(Response(completion, result))));
                    }
                    Operation::Job {
                        session,
                        csrf,
                        key,
                        command: JobCommand::Export(command),
                    } => {
                        // Probe at startup, never touch813 tables on an older
                        // valid runtime. Historical resolution needs no root.
                        if !self.export_jobs {
                            return Ok(Execution::Complete(Some(Response(
                                completion,
                                Err(SessionError::ArchiveUnavailable),
                            ))));
                        }
                        let commit = std::cell::Cell::new(false);
                        let result = sql(
                            maximum,
                            owner
                                .accept_export(&session, &csrf, key, &command, None, None, &commit),
                        )
                        .await?;
                        if let Some(destination) = destination
                            && native_reader
                            && matches!(result, Err(SessionError::ArchiveUnavailable))
                        {
                            return Ok(Execution::Archive(AllocationWork {
                                completion,
                                session,
                                csrf,
                                key,
                                kind: AllocationKind::Export {
                                    command,
                                    destination: destination.clone(),
                                },
                            }));
                        }
                        return Ok(Execution::Complete(Some(Response(
                            completion,
                            result.map(Reply::Job),
                        ))));
                    }
                    Operation::History {
                        session,
                        csrf,
                        query,
                        delivery,
                        queue_permit,
                    } => {
                        drop(queue_permit);
                        return Ok(Execution::History(HistoryWork {
                            completion,
                            session,
                            csrf,
                            query,
                            delivery,
                        }));
                    }
                    Operation::Grant {
                        session,
                        csrf,
                        key,
                        command,
                        proof,
                    } => {
                        // Historical retries and revocation need no Archive.
                        // An unconfigured native reader cannot enable a grant.
                        let commit = std::cell::Cell::new(false);
                        let result = sql(
                            maximum,
                            owner.accept_scope_grant(
                                &session, &csrf, key, &command, proof, None, &commit,
                            ),
                        )
                        .await?;
                        if native_reader && matches!(result, Err(SessionError::ArchiveUnavailable))
                        {
                            return Ok(Execution::Archive(AllocationWork {
                                completion,
                                session,
                                csrf,
                                key,
                                kind: AllocationKind::Grant { command, proof },
                            }));
                        }
                        return Ok(Execution::Complete(Some(Response(
                            completion,
                            result.map(Reply::Grant),
                        ))));
                    }
                    Operation::Reauthenticate {
                        session,
                        csrf,
                        key,
                        command,
                        password,
                        peer,
                    } => {
                        if !self.hashes.is_empty() {
                            return Ok(Execution::Complete(Some(Response(
                                completion,
                                Err(SessionError::Capacity),
                            ))));
                        }
                        let Ok(mut grant) = self.budget.try_acquire(
                            ResourceRequest::data(HASH_RESERVATION, 1, 0)
                                .expect("fixed hash reservation"),
                        ) else {
                            return Ok(Execution::Complete(Some(Response(
                                completion,
                                Err(SessionError::Capacity),
                            ))));
                        };
                        let challenge = sql(
                            maximum,
                            owner.begin_browser_reauthentication(
                                &session, &csrf, key, &command, peer,
                            ),
                        )
                        .await?;
                        match challenge {
                            Ok(challenge) if !completion.abandoned() => {
                                self.hashes.spawn_blocking(move || {
                                    let result = challenge
                                        .verify(password.as_bytes(), &mut grant)
                                        .map(VerifiedPassword::Reauthentication);
                                    drop(password);
                                    drop(grant);
                                    (completion, result)
                                });
                            }
                            Ok(_) => {}
                            Err(e) => {
                                return Ok(Execution::Complete(Some(Response(completion, Err(e)))));
                            }
                        }
                    }
                    Operation::Login {
                        username,
                        password,
                        peer,
                    } => {
                        if !self.hashes.is_empty() {
                            return Ok(Execution::Complete(Some(Response(
                                completion,
                                Err(SessionError::Capacity),
                            ))));
                        }
                        let Ok(mut grant) = self.budget.try_acquire(
                            ResourceRequest::data(HASH_RESERVATION, 1, 0)
                                .expect("fixed hash reservation"),
                        ) else {
                            return Ok(Execution::Complete(Some(Response(
                                completion,
                                Err(SessionError::Capacity),
                            ))));
                        };
                        let challenge =
                            sql(maximum, owner.begin_browser_login(&username, peer)).await?;
                        match challenge {
                            Ok(challenge) if !completion.abandoned() => {
                                self.hashes.spawn_blocking(move || {
                                    let result = challenge
                                        .verify(password.as_bytes(), &mut grant)
                                        .map(VerifiedPassword::Login);
                                    drop(password);
                                    drop(grant);
                                    (completion, result)
                                });
                            }
                            Ok(_) => {}
                            Err(e) => {
                                return Ok(Execution::Complete(Some(Response(completion, Err(e)))));
                            }
                        }
                    }
                    Operation::Me(token) => {
                        let result =
                            sql(maximum, owner.inspect_browser_session(&token, true)).await?;
                        return Ok(Execution::Complete(Some(Response(
                            completion,
                            result.map(Reply::Me),
                        ))));
                    }
                    Operation::Logout { session, csrf } => {
                        let result =
                            sql(maximum, owner.revoke_browser_session(&session, &csrf)).await?;
                        return Ok(Execution::Complete(Some(Response(
                            completion,
                            result.map(|()| Reply::Logout),
                        ))));
                    }
                }
            }
        }
        Ok(Execution::Complete(None))
    }

    pub fn renew_delivery(&self, window: DeliveryWindow) -> Result<()> {
        self.delivery
            .renew(window)
            .map_err(|_| (4, "private delivery authority closed".into()))
    }
    pub fn stop_admission(&self) {
        self.delivery.revoke();
        self.server.request_stop();
    }
    pub async fn stop(&mut self, grace: Duration) -> Result<()> {
        self.stop_admission();
        let sockets = self.server.stop().await;
        // HTTP cancellation never detaches expensive password work. If the
        // bounded join fails, the caller must terminate this process; it cannot
        // report a graceful release while the blocking task remains alive.
        let joined = tokio::time::timeout(grace, async {
            while let Some(result) = self.hashes.join_next().await {
                drop(result.map_err(|_| (1, "private password worker failed".into()))?);
            }
            Ok::<_, (u8, String)>(())
        })
        .await
        .map_err(|_| {
            (
                4,
                "private password worker did not join; process termination required".into(),
            )
        })?;
        joined?;
        sockets.map_err(|_| (1, "private HTTP task termination failed".into()))
    }
}

async fn sql<T>(
    maximum: Duration,
    future: impl Future<Output = std::result::Result<T, SessionError>>,
) -> Result<std::result::Result<T, SessionError>> {
    // Once admitted, SQL is collected under its authority budget even if the
    // HTTP deadline/disconnect makes the response undeliverable. A slow client
    // cannot force a healthy transaction cancellation at its own deadline.
    let result = tokio::time::timeout(maximum, future)
        .await
        .map_err(|_| (4, "private management SQL outcome unresolved".into()))?;
    match result {
        Err(
            e @ (SessionError::Backend
            | SessionError::Prerequisite
            | SessionError::InvalidMetadata
            | SessionError::Unavailable
            | SessionError::CommitUnknown
            | SessionError::PolicyChanged),
        ) => Err((4, format!("private management backend unavailable: {e:?}"))),
        result => Ok(result),
    }
}
#[cfg(test)]
mod tests;
