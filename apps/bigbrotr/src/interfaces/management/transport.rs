//! Owned HTTP1 connections. The Control supervisor consumes bounded commands;
//! HTTP tasks never borrow its connection, policy tracker or archive custody.
use super::{
    Assets, BrowserBoundary, Profile,
    boundary::single,
    delivery::{ActionGate, DeliveryGate, GuardedSocket},
};
use axum::{
    Extension, Json, Router,
    body::{Body, to_bytes},
    extract::{Request, State},
    http::{HeaderMap, HeaderValue, Method, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{delete, get, post, put},
};
use bigbrotr::{
    management::{
        grants::{CommandKey, GrantCommand, GrantReceipt},
        job_list::{self, JobListRequest, PageBody},
        jobs::{JobCommand, JobObservation, JobReceipt, ResumeExportCommand, ResumeExportReceipt},
        reauthentication::ReauthenticationProof,
        relay_enrollment::{EnrollmentCommand, EnrollmentReceipt},
        sessions::{BrowserSecret, IssuedSession, SessionError, SessionObservation},
    },
    runtime::{OwnedPermit, ResourceBudget, ResourceRequest},
};
use hyper::server::conn::http1;
use hyper_util::{
    rt::{TokioIo, TokioTimer},
    service::TowerToHyperService,
};
use serde::Deserialize;
use serde_json::json;
use std::{
    io,
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::Duration,
};
use tokio::{
    net::TcpListener,
    sync::{mpsc, oneshot, watch},
    task::{JoinHandle, JoinSet},
    time::Instant,
};
use zeroize::Zeroizing;

pub(super) const CONNECTIONS: usize = 16;
pub(super) const QUEUE: usize = 16;
pub(super) const CONNECTION_BYTES: u64 = 128 * 1024;
pub(super) const REQUEST_BYTES: u64 = 64 * 1024;
const BODY_BYTES: usize = 8 * 1024;
const CONNECTION_TIME: Duration = Duration::from_secs(5);
const SESSION_COOKIE: &str = "BigBrotrLocalSession";
const CSRF_COOKIE: &str = "BigBrotrLocalCsrf";

pub(super) enum Operation {
    RelayEnrollment {
        session: BrowserSecret,
        csrf: BrowserSecret,
        key: CommandKey,
        command: EnrollmentCommand,
    },
    JobList {
        session: BrowserSecret,
        csrf: BrowserSecret,
        request: JobListRequest,
        delivery: ConnectionDelivery,
    },
    ResumeExport {
        session: BrowserSecret,
        csrf: BrowserSecret,
        key: CommandKey,
        job: bigbrotr_model::JobId,
        command: ResumeExportCommand,
    },
    JobStatus {
        session: BrowserSecret,
        csrf: BrowserSecret,
        job: bigbrotr_model::JobId,
        delivery: ConnectionDelivery,
    },
    Job {
        session: BrowserSecret,
        csrf: BrowserSecret,
        key: CommandKey,
        command: JobCommand,
    },
    History {
        session: BrowserSecret,
        csrf: BrowserSecret,
        query: bigbrotr_model::read::QueryRequest,
        delivery: ConnectionDelivery,
        queue_permit: tokio::sync::OwnedSemaphorePermit,
    },
    Grant {
        session: BrowserSecret,
        csrf: BrowserSecret,
        key: CommandKey,
        command: GrantCommand,
        proof: bigbrotr_model::ReauthenticationId,
    },
    Reauthenticate {
        session: BrowserSecret,
        csrf: BrowserSecret,
        key: CommandKey,
        command: GrantCommand,
        password: Zeroizing<String>,
        peer: IpAddr,
    },
    Login {
        username: String,
        password: Zeroizing<String>,
        peer: IpAddr,
    },
    Me(BrowserSecret),
    Logout {
        session: BrowserSecret,
        csrf: BrowserSecret,
    },
}
pub(super) enum Reply {
    RelayEnrollment(EnrollmentReceipt),
    JobList(PageBody),
    ResumeExport(ResumeExportReceipt),
    JobStatus(Option<JobObservation>),
    Job(JobReceipt),
    History(Box<super::history::HistoryBody>),
    Grant(GrantReceipt),
    Reauthenticated(ReauthenticationProof),
    Login(IssuedSession),
    Me(SessionObservation),
    Logout,
}
pub(super) struct Completion {
    pub deadline: Instant,
    pub reply: oneshot::Sender<Result<Reply, SessionError>>,
    // This reservation moves with queued/SQL/hash work even if HTTP disconnects.
    pub request_grant: OwnedPermit,
}
pub(super) struct Command {
    pub operation: Operation,
    pub completion: Completion,
}
impl Completion {
    pub fn abandoned(&self) -> bool {
        self.reply.is_closed() || Instant::now() >= self.deadline
    }
    pub fn finish(self, result: Result<Reply, SessionError>) {
        let _ = self.reply.send(result);
        // The actual connection reserves response-buffer memory until IO drops.
        drop(self.request_grant);
    }
}

#[derive(Clone)]
struct Shared {
    boundary: BrowserBoundary,
    budget: ResourceBudget,
    commands: mpsc::Sender<Command>,
    assets: Option<Arc<Assets>>,
    history: HistoryLimits,
    history_slots: Arc<tokio::sync::Semaphore>,
}
#[derive(Clone, Copy)]
pub(super) struct HistoryLimits {
    pub bytes: usize,
    pub default_page: u16,
    pub max_page: u16,
    pub queue: usize,
}
impl Default for HistoryLimits {
    fn default() -> Self {
        Self {
            bytes: BODY_BYTES,
            default_page: 100,
            max_page: 1000,
            queue: QUEUE,
        }
    }
}
impl HistoryLimits {
    pub fn configured(root: &crate::config::Resolved) -> Self {
        Self {
            bytes: root.number("read.request.max_bytes").min(BODY_BYTES as u64) as usize,
            default_page: root.number("read.default_page_size").min(1000) as u16,
            max_page: root.number("read.max_page_size").min(1000) as u16,
            queue: root.number("read.admission.queue_max").min(QUEUE as u64) as usize,
        }
    }
}

#[derive(Clone)]
struct Connection {
    peer: IpAddr,
    deadline: Instant,
    delivery: ConnectionDelivery,
}

pub(super) struct Server {
    pub commands: mpsc::Receiver<Command>,
    shutdown: watch::Sender<bool>,
    task: Option<JoinHandle<io::Result<()>>>,
}
impl Server {
    pub async fn bind(
        address: SocketAddr,
        budget: ResourceBudget,
        delivery: DeliveryGate,
        assets: Option<Assets>,
        history: HistoryLimits,
        profile: Profile,
    ) -> io::Result<Self> {
        BrowserBoundary::loopback(address).map_err(io::Error::other)?;
        let listener = TcpListener::bind(address).await?;
        Self::from_listener_limits(listener, budget, delivery, assets, history, profile)
    }
    #[cfg(test)]
    pub(super) fn from_listener(
        listener: TcpListener,
        budget: ResourceBudget,
        delivery: DeliveryGate,
        assets: Option<Assets>,
    ) -> io::Result<Self> {
        Self::from_listener_profile(
            listener,
            budget,
            delivery,
            assets,
            Profile::Operational {
                export_jobs: true,
                export_resume: true,
                job_pages: true,
            },
        )
    }
    #[cfg(test)]
    pub(super) fn from_listener_profile(
        listener: TcpListener,
        budget: ResourceBudget,
        delivery: DeliveryGate,
        assets: Option<Assets>,
        profile: Profile,
    ) -> io::Result<Self> {
        Self::from_listener_limits(
            listener,
            budget,
            delivery,
            assets,
            HistoryLimits::default(),
            profile,
        )
    }
    fn from_listener_limits(
        listener: TcpListener,
        budget: ResourceBudget,
        delivery: DeliveryGate,
        assets: Option<Assets>,
        history: HistoryLimits,
        profile: Profile,
    ) -> io::Result<Self> {
        if matches!(profile, Profile::Initial { .. }) && assets.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "initial profile has no assets",
            ));
        }
        let boundary =
            BrowserBoundary::loopback(listener.local_addr()?).map_err(io::Error::other)?;
        let (sender, commands) = mpsc::channel(QUEUE);
        let (shutdown, stopped) = watch::channel(false);
        let state = Shared {
            boundary,
            budget,
            commands: sender,
            assets: assets.map(Arc::new),
            history,
            history_slots: Arc::new(tokio::sync::Semaphore::new(history.queue)),
        };
        let mut router = Router::new()
            .route("/management/v1/sessions", post(login))
            .route("/management/v1/sessions/current", delete(logout))
            .route("/management/v1/me", get(me));
        if matches!(profile, Profile::Initial { .. }) {
            router = router.route("/management/v1/relay-enrollments", post(enroll_relay));
        }
        if matches!(profile, Profile::Operational { .. }) {
            router = router
                .route(
                    "/management/v1/sessions/current/reauthenticate",
                    post(reauthenticate),
                )
                .route("/management/v1/jobs", post(create_job).get(list_jobs))
                .route("/management/v1/jobs/{job_id}", get(job_status))
                .route("/management/v1/jobs/{job_id}/resume", post(resume_export))
                .route(
                    "/v1/resources/event_history",
                    get(history_query).post(history_query),
                )
                .route(
                    "/management/v1/principals/{principal_id}/grants",
                    put(change_grant),
                );
        }
        let router = router.fallback(static_asset).with_state(state.clone());
        let task = tokio::spawn(serve(listener, router, state.budget, stopped, delivery));
        Ok(Self {
            commands,
            shutdown,
            task: Some(task),
        })
    }
    /// Signal admission/IO shutdown before waiting for expensive owned work.
    pub fn request_stop(&self) {
        self.shutdown.send_replace(true);
    }
    pub fn stop_signal(&self) -> watch::Sender<bool> {
        self.shutdown.clone()
    }
    /// All connection futures own their IO and grant and are collected here.
    /// Hash work belongs to the Control supervisor and must be joined separately.
    pub async fn stop(&mut self) -> io::Result<()> {
        self.commands.close();
        self.request_stop();
        // Dropped queued commands release only their own reserved request data.
        while let Ok(command) = self.commands.try_recv() {
            drop(command);
        }
        match self.task.take() {
            Some(task) => task.await.map_err(io::Error::other)?,
            None => Ok(()),
        }
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        self.shutdown.send_replace(true);
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

async fn serve(
    listener: TcpListener,
    router: Router,
    budget: ResourceBudget,
    mut stopped: watch::Receiver<bool>,
    delivery: DeliveryGate,
) -> io::Result<()> {
    let mut connections = JoinSet::new();
    let (all_joined, joined) = watch::channel(false);
    let outcome = loop {
        tokio::select! {
            biased;
            _ = stopped.changed() => break Ok(()),
            result = connections.join_next(), if !connections.is_empty() => {
                if result.is_some_and(|r| r.is_err()) { break Err(io::Error::other("private HTTP connection task failed")); }
            },
            accepted = listener.accept() => {
                let (socket, peer) = match accepted { Ok(v) => v, Err(e) => break Err(e) };
                if connections.len() >= CONNECTIONS { drop(socket); continue; }
                let Ok(grant) = budget.try_acquire(ResourceRequest::data(CONNECTION_BYTES, 1, 0).expect("fixed connection reservation")) else { drop(socket); continue; };
                let (ended, terminal) = watch::channel(false);
                let action = ActionGate::new();
                let connection = Connection { peer: peer.ip(), deadline: Instant::now() + CONNECTION_TIME,
                    delivery: ConnectionDelivery { action: action.clone(), terminal, joined: joined.clone() } };
                let service = TowerToHyperService::new(router.clone().layer(Extension(connection.clone())));
                let socket=GuardedSocket::new(socket,delivery.clone(),action);
                connections.spawn(async move {
                    let mut http = http1::Builder::new();
                    http.keep_alive(false).half_close(false).max_headers(32).max_buf_size(16 * 1024)
                        .header_read_timeout(Duration::from_secs(2)).timer(TokioTimer::new());
                    // No upgrade path or detached hyper/axum serve task. Timeout
                    // drops the actual IO, including an incomplete body/write.
                    let _ = tokio::time::timeout_at(connection.deadline, http.serve_connection(TokioIo::new(socket), service)).await;
                    drop(grant);
                    ended.send_replace(true);
                });
            },
        }
    };
    drop(listener);
    connections.abort_all();
    while connections.join_next().await.is_some() {}
    all_joined.send_replace(true);
    outcome
}

/// Closed notification channels alone are not proof of IO destruction. Normal
/// completion signals after the owned HTTP future drops; aborted/panicked tasks
/// require the server's positive abort-and-join acknowledgement.
#[derive(Clone)]
pub(crate) struct ConnectionDelivery {
    pub action: ActionGate,
    terminal: watch::Receiver<bool>,
    joined: watch::Receiver<bool>,
}
impl ConnectionDelivery {
    pub async fn ended(&mut self) {
        let mut terminal_open = true;
        let mut joined_open = true;
        loop {
            if *self.terminal.borrow() || *self.joined.borrow() {
                return;
            }
            tokio::select! {
                result = self.terminal.changed(), if terminal_open => { terminal_open = result.is_ok(); },
                result = self.joined.changed(), if joined_open => { joined_open = result.is_ok(); },
                else => std::future::pending::<()>().await,
            }
        }
    }
}

async fn history_query(
    State(shared): State<Shared>,
    Extension(connection): Extension<Connection>,
    request: Request,
) -> Response {
    if let Err(status) = shared
        .boundary
        .check(request.method(), request.uri(), request.headers())
    {
        return error(status);
    }
    let grant = match shared
        .budget
        .try_acquire(ResourceRequest::data(REQUEST_BYTES, 1, 0).expect("fixed request reservation"))
    {
        Ok(grant) => grant,
        Err(_) => return error(StatusCode::TOO_MANY_REQUESTS),
    };
    let session = match session(request.headers()) {
        Ok(value) => value,
        Err(status) => return error(status),
    };
    let Some(csrf) =
        single(request.headers(), "x-csrf-token").and_then(|v| BrowserSecret::parse_hex(v).ok())
    else {
        return error(StatusCode::FORBIDDEN);
    };
    let decoded = if request.method() == Method::GET {
        if request.uri().query().unwrap_or("").len() > shared.history.bytes {
            return error(StatusCode::PAYLOAD_TOO_LARGE);
        }
        let result = bigbrotr_protocol::read::decode_get(request.uri().query().unwrap_or(""));
        if !matches!(to_bytes(request.into_body(), 0).await, Ok(bytes) if bytes.is_empty()) {
            return error(StatusCode::BAD_REQUEST);
        }
        result
    } else if request.method() == Method::POST {
        if request.uri().query().is_some() {
            return error(StatusCode::BAD_REQUEST);
        }
        if !single(request.headers(), "content-type").is_some_and(|v| {
            v.eq_ignore_ascii_case(b"application/json")
                || v.eq_ignore_ascii_case(b"application/json; charset=utf-8")
        }) {
            return error(StatusCode::UNSUPPORTED_MEDIA_TYPE);
        }
        let bytes = match to_bytes(request.into_body(), shared.history.bytes).await {
            Ok(value) => value,
            Err(_) => return error(StatusCode::PAYLOAD_TOO_LARGE),
        };
        bigbrotr_protocol::read::decode_post(&bytes)
    } else {
        return protect(StatusCode::METHOD_NOT_ALLOWED.into_response());
    };
    let mut query = match decoded {
        Ok(query) => query,
        Err(bigbrotr_protocol::read::QueryError::Unsupported) => {
            return failure(
                StatusCode::BAD_REQUEST,
                "unsupported_query",
                "This query profile is unavailable.",
                "correct_request",
            );
        }
        Err(bigbrotr_protocol::read::QueryError::Limit) => {
            return error(StatusCode::PAYLOAD_TOO_LARGE);
        }
        Err(_) => return error(StatusCode::BAD_REQUEST),
    };
    let page = query.page_size.unwrap_or(shared.history.default_page);
    if page > shared.history.max_page {
        return error(StatusCode::BAD_REQUEST);
    }
    query.page_size = Some(page);
    let delivery = connection.delivery.clone();
    let queue_permit = match shared.history_slots.clone().try_acquire_owned() {
        Ok(value) => value,
        Err(_) => return error(StatusCode::TOO_MANY_REQUESTS),
    };
    dispatch(
        &shared,
        connection,
        grant,
        Operation::History {
            session,
            csrf,
            query,
            delivery,
            queue_permit,
        },
    )
    .await
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LoginInput {
    username: String,
    password: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReauthenticationInput {
    password: String,
    command: GrantCommand,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GrantInput {
    proof_id: bigbrotr_model::ReauthenticationId,
    command: GrantCommand,
}

async fn list_jobs(
    State(shared): State<Shared>,
    Extension(connection): Extension<Connection>,
    request: Request,
) -> Response {
    let grant = match begin_query(&shared, &request) {
        Ok(grant) => grant,
        Err(status) => return error(status),
    };
    if request.method() != Method::GET {
        return protect(
            (
                StatusCode::METHOD_NOT_ALLOWED,
                [(header::ALLOW, "GET, POST")],
            )
                .into_response(),
        );
    }
    let session = match session(request.headers()) {
        Ok(value) => value,
        Err(status) => return error(status),
    };
    let Some(csrf) =
        single(request.headers(), "x-csrf-token").and_then(|v| BrowserSecret::parse_hex(v).ok())
    else {
        return error(StatusCode::FORBIDDEN);
    };
    let parsed = match job_list::parse_request(request.uri().query().unwrap_or("")) {
        Ok(value) => value,
        Err(_) => return error(StatusCode::BAD_REQUEST),
    };
    if !matches!(to_bytes(request.into_body(), 0).await, Ok(bytes) if bytes.is_empty()) {
        return error(StatusCode::BAD_REQUEST);
    }
    let delivery = connection.delivery.clone();
    dispatch(
        &shared,
        connection,
        grant,
        Operation::JobList {
            session,
            csrf,
            request: parsed,
            delivery,
        },
    )
    .await
}

async fn job_status(
    State(shared): State<Shared>,
    Extension(connection): Extension<Connection>,
    request: Request,
) -> Response {
    let grant = match begin(&shared, &request) {
        Ok(grant) => grant,
        Err(status) => return error(status),
    };
    // Axum GET also handles HEAD; this observation profile accepts only GET.
    if request.method() != Method::GET {
        return protect((StatusCode::METHOD_NOT_ALLOWED, [(header::ALLOW, "GET")]).into_response());
    }
    let session = match session(request.headers()) {
        Ok(value) => value,
        Err(status) => return error(status),
    };
    let Some(csrf) =
        single(request.headers(), "x-csrf-token").and_then(|v| BrowserSecret::parse_hex(v).ok())
    else {
        return error(StatusCode::FORBIDDEN);
    };
    let Some(job) = request
        .uri()
        .path()
        .strip_prefix("/management/v1/jobs/")
        .and_then(|v| v.parse::<bigbrotr_model::JobId>().ok())
    else {
        return error(StatusCode::BAD_REQUEST);
    };
    if !matches!(to_bytes(request.into_body(), 0).await, Ok(bytes) if bytes.is_empty()) {
        return error(StatusCode::BAD_REQUEST);
    }
    let delivery = connection.delivery.clone();
    dispatch(
        &shared,
        connection,
        grant,
        Operation::JobStatus {
            session,
            csrf,
            job,
            delivery,
        },
    )
    .await
}

async fn resume_export(
    State(shared): State<Shared>,
    Extension(connection): Extension<Connection>,
    request: Request,
) -> Response {
    let grant = match begin(&shared, &request) {
        Ok(grant) => grant,
        Err(status) => return error(status),
    };
    let session = match session(request.headers()) {
        Ok(session) => session,
        Err(status) => return error(status),
    };
    let Some(csrf) =
        single(request.headers(), "x-csrf-token").and_then(|v| BrowserSecret::parse_hex(v).ok())
    else {
        return error(StatusCode::FORBIDDEN);
    };
    let Some(key) = single(request.headers(), "idempotency-key")
        .and_then(|v| std::str::from_utf8(v).ok())
        .and_then(|v| v.parse::<CommandKey>().ok())
    else {
        return error(StatusCode::BAD_REQUEST);
    };
    let Some(job) = request
        .uri()
        .path()
        .strip_prefix("/management/v1/jobs/")
        .and_then(|v| v.strip_suffix("/resume"))
        .and_then(|v| v.parse::<bigbrotr_model::JobId>().ok())
    else {
        return error(StatusCode::BAD_REQUEST);
    };
    if !single(request.headers(), "content-type").is_some_and(|v| {
        v.eq_ignore_ascii_case(b"application/json")
            || v.eq_ignore_ascii_case(b"application/json; charset=utf-8")
    }) {
        return error(StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }
    let bytes = match to_bytes(request.into_body(), BODY_BYTES).await {
        Ok(bytes) => bytes,
        Err(_) => return error(StatusCode::PAYLOAD_TOO_LARGE),
    };
    let command = match serde_json::from_slice::<ResumeExportCommand>(&bytes) {
        Ok(command) => command,
        Err(_) => return error(StatusCode::BAD_REQUEST),
    };
    drop(bytes);
    dispatch(
        &shared,
        connection,
        grant,
        Operation::ResumeExport {
            session,
            csrf,
            key,
            job,
            command,
        },
    )
    .await
}

async fn enroll_relay(
    State(shared): State<Shared>,
    Extension(connection): Extension<Connection>,
    request: Request,
) -> Response {
    let grant = match begin(&shared, &request) {
        Ok(grant) => grant,
        Err(status) => return error(status),
    };
    let session = match session(request.headers()) {
        Ok(session) => session,
        Err(status) => return error(status),
    };
    let Some(csrf) =
        single(request.headers(), "x-csrf-token").and_then(|v| BrowserSecret::parse_hex(v).ok())
    else {
        return error(StatusCode::FORBIDDEN);
    };
    let Some(key) = single(request.headers(), "idempotency-key")
        .and_then(|v| std::str::from_utf8(v).ok())
        .and_then(|v| v.parse::<CommandKey>().ok())
    else {
        return error(StatusCode::BAD_REQUEST);
    };
    if !single(request.headers(), "content-type").is_some_and(|v| {
        v.eq_ignore_ascii_case(b"application/json")
            || v.eq_ignore_ascii_case(b"application/json; charset=utf-8")
    }) {
        return error(StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }
    let bytes = match to_bytes(request.into_body(), BODY_BYTES).await {
        Ok(bytes) => bytes,
        Err(_) => return error(StatusCode::PAYLOAD_TOO_LARGE),
    };
    let command = match serde_json::from_slice::<EnrollmentCommand>(&bytes) {
        Ok(command) => command,
        Err(_) => return error(StatusCode::BAD_REQUEST),
    };
    drop(bytes);
    // Shape only; Control separately checks the installed dataset and authority.
    if command.validate(command.visibility.dataset_id).is_err() {
        return error(StatusCode::BAD_REQUEST);
    }
    dispatch(
        &shared,
        connection,
        grant,
        Operation::RelayEnrollment {
            session,
            csrf,
            key,
            command,
        },
    )
    .await
}

async fn create_job(
    State(shared): State<Shared>,
    Extension(connection): Extension<Connection>,
    request: Request,
) -> Response {
    let grant = match begin(&shared, &request) {
        Ok(grant) => grant,
        Err(status) => return error(status),
    };
    let session = match session(request.headers()) {
        Ok(session) => session,
        Err(status) => return error(status),
    };
    let Some(csrf) =
        single(request.headers(), "x-csrf-token").and_then(|v| BrowserSecret::parse_hex(v).ok())
    else {
        return error(StatusCode::FORBIDDEN);
    };
    let Some(key) = single(request.headers(), "idempotency-key")
        .and_then(|v| std::str::from_utf8(v).ok())
        .and_then(|v| v.parse::<CommandKey>().ok())
    else {
        return error(StatusCode::BAD_REQUEST);
    };
    if !single(request.headers(), "content-type").is_some_and(|v| {
        v.eq_ignore_ascii_case(b"application/json")
            || v.eq_ignore_ascii_case(b"application/json; charset=utf-8")
    }) {
        return error(StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }
    let bytes = match to_bytes(request.into_body(), BODY_BYTES).await {
        Ok(bytes) => bytes,
        Err(_) => return error(StatusCode::PAYLOAD_TOO_LARGE),
    };
    let command: JobCommand = match serde_json::from_slice(&bytes) {
        Ok(value) => value,
        Err(_) => return error(StatusCode::BAD_REQUEST),
    };
    drop(bytes);
    // Reject unsupported shapes here; Control separately binds the authenticated
    // actor, installed dataset, revisions and exact native/destination proofs.
    let validation = match &command {
        JobCommand::Export(command) => command.validate(command.query.scope.dataset_id),
    };
    if let Err(error_kind) = validation {
        return error(if error_kind == SessionError::Capacity {
            StatusCode::TOO_MANY_REQUESTS
        } else {
            StatusCode::BAD_REQUEST
        });
    }
    dispatch(
        &shared,
        connection,
        grant,
        Operation::Job {
            session,
            csrf,
            key,
            command,
        },
    )
    .await
}

async fn change_grant(
    State(shared): State<Shared>,
    Extension(connection): Extension<Connection>,
    request: Request,
) -> Response {
    let grant = match begin(&shared, &request) {
        Ok(grant) => grant,
        Err(status) => return error(status),
    };
    let session = match session(request.headers()) {
        Ok(session) => session,
        Err(status) => return error(status),
    };
    let Some(csrf) =
        single(request.headers(), "x-csrf-token").and_then(|v| BrowserSecret::parse_hex(v).ok())
    else {
        return error(StatusCode::FORBIDDEN);
    };
    let Some(key) = single(request.headers(), "idempotency-key")
        .and_then(|v| std::str::from_utf8(v).ok())
        .and_then(|v| v.parse::<CommandKey>().ok())
    else {
        return error(StatusCode::BAD_REQUEST);
    };
    // Require the canonical raw path: percent-encoding cannot introduce a
    // second spelling or redirect a command to a different target principal.
    let Some(principal) = request
        .uri()
        .path()
        .strip_prefix("/management/v1/principals/")
        .and_then(|v| v.strip_suffix("/grants"))
        .and_then(|v| v.parse::<bigbrotr_model::PrincipalId>().ok())
    else {
        return error(StatusCode::BAD_REQUEST);
    };
    if !single(request.headers(), "content-type").is_some_and(|v| {
        v.eq_ignore_ascii_case(b"application/json")
            || v.eq_ignore_ascii_case(b"application/json; charset=utf-8")
    }) {
        return error(StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }
    let bytes = match to_bytes(request.into_body(), BODY_BYTES).await {
        Ok(bytes) => bytes,
        Err(_) => return error(StatusCode::PAYLOAD_TOO_LARGE),
    };
    let input: GrantInput = match serde_json::from_slice(&bytes) {
        Ok(input) => input,
        Err(_) => return error(StatusCode::BAD_REQUEST),
    };
    drop(bytes);
    if principal != input.command.principal_id {
        return error(StatusCode::BAD_REQUEST);
    }
    dispatch(
        &shared,
        connection,
        grant,
        Operation::Grant {
            session,
            csrf,
            key,
            command: input.command,
            proof: input.proof_id,
        },
    )
    .await
}

async fn reauthenticate(
    State(shared): State<Shared>,
    Extension(connection): Extension<Connection>,
    request: Request,
) -> Response {
    let grant = match begin(&shared, &request) {
        Ok(g) => g,
        Err(e) => return error(e),
    };
    let session = match session(request.headers()) {
        Ok(t) => t,
        Err(e) => return error(e),
    };
    let Some(csrf) =
        single(request.headers(), "x-csrf-token").and_then(|v| BrowserSecret::parse_hex(v).ok())
    else {
        return error(StatusCode::FORBIDDEN);
    };
    let Some(key) = single(request.headers(), "idempotency-key")
        .and_then(|v| std::str::from_utf8(v).ok())
        .and_then(|v| v.parse::<CommandKey>().ok())
    else {
        return error(StatusCode::BAD_REQUEST);
    };
    if !single(request.headers(), "content-type").is_some_and(|v| {
        v.eq_ignore_ascii_case(b"application/json")
            || v.eq_ignore_ascii_case(b"application/json; charset=utf-8")
    }) {
        return error(StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }
    let bytes = match to_bytes(request.into_body(), BODY_BYTES).await {
        Ok(bytes) => bytes,
        Err(_) => return error(StatusCode::PAYLOAD_TOO_LARGE),
    };
    let input: ReauthenticationInput = match serde_json::from_slice(&bytes) {
        Ok(input) => input,
        Err(_) => return error(StatusCode::BAD_REQUEST),
    };
    drop(bytes);
    let password = Zeroizing::new(input.password);
    if password.len() > 1024 {
        return error(StatusCode::BAD_REQUEST);
    }
    dispatch(
        &shared,
        connection.clone(),
        grant,
        Operation::Reauthenticate {
            session,
            csrf,
            key,
            command: input.command,
            password,
            peer: connection.peer,
        },
    )
    .await
}

fn begin(shared: &Shared, request: &Request) -> Result<OwnedPermit, StatusCode> {
    let grant = begin_query(shared, request)?;
    if request.uri().query().is_some() {
        return Err(StatusCode::BAD_REQUEST);
    }
    Ok(grant)
}
fn begin_query(shared: &Shared, request: &Request) -> Result<OwnedPermit, StatusCode> {
    shared
        .boundary
        .check(request.method(), request.uri(), request.headers())?;
    shared
        .budget
        .try_acquire(ResourceRequest::data(REQUEST_BYTES, 1, 0).expect("fixed request reservation"))
        .map_err(|_| StatusCode::TOO_MANY_REQUESTS)
}

async fn static_asset(State(shared): State<Shared>, request: Request) -> Response {
    let _grant = match begin(&shared, &request) {
        Ok(grant) => grant,
        Err(status) => return error(status),
    };
    if !matches!(*request.method(), Method::GET | Method::HEAD) {
        return protect(
            (
                StatusCode::METHOD_NOT_ALLOWED,
                [(header::ALLOW, "GET, HEAD")],
            )
                .into_response(),
        );
    }
    let Some(asset) = shared
        .assets
        .as_ref()
        .and_then(|assets| assets.lookup(request.uri().path()))
    else {
        return protect((StatusCode::NOT_FOUND, "Private asset unavailable.").into_response());
    };
    let body = if request.method() == Method::HEAD {
        Body::empty()
    } else {
        Body::from(asset.body.clone())
    };
    let mut response = Response::new(body);
    let headers = response.headers_mut();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static(asset.mime));
    headers.insert(header::CONTENT_LENGTH, HeaderValue::from(asset.body.len()));
    for (name, value) in [
        ("cache-control", "no-store"),
        ("x-content-type-options", "nosniff"),
        ("x-frame-options", "DENY"),
        // Fetch serializes Origin as null for same-origin POST/DELETE under
        // no-referrer. Keep usable same-origin commands without cross-origin
        // referrer disclosure; the HTML meta must agree with this policy.
        ("referrer-policy", "same-origin"),
        ("cross-origin-resource-policy", "same-origin"),
        // The build supplies script/style hashes in HTML meta. frame-ancestors
        // is ignored in meta and must additionally be enforced by this header.
        (
            "content-security-policy",
            "frame-ancestors 'none'; base-uri 'none'; object-src 'none'",
        ),
    ] {
        headers.insert(name, HeaderValue::from_static(value));
    }
    response
}
async fn login(
    State(shared): State<Shared>,
    Extension(connection): Extension<Connection>,
    request: Request,
) -> Response {
    let grant = match begin(&shared, &request) {
        Ok(g) => g,
        Err(e) => return error(e),
    };
    let content_type = single(request.headers(), "content-type");
    if !content_type.is_some_and(|v| {
        v.eq_ignore_ascii_case(b"application/json")
            || v.eq_ignore_ascii_case(b"application/json; charset=utf-8")
    }) {
        return error(StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }
    let bytes = match to_bytes(request.into_body(), BODY_BYTES).await {
        Ok(bytes) => bytes,
        Err(_) => return error(StatusCode::PAYLOAD_TOO_LARGE),
    };
    let input: LoginInput = match serde_json::from_slice(&bytes) {
        Ok(input) => input,
        Err(_) => return error(StatusCode::BAD_REQUEST),
    };
    drop(bytes);
    let password = Zeroizing::new(input.password);
    if input.username.len() > 64 || password.len() > 1024 {
        return error(StatusCode::BAD_REQUEST);
    }
    dispatch(
        &shared,
        connection.clone(),
        grant,
        Operation::Login {
            username: input.username,
            password,
            peer: connection.peer,
        },
    )
    .await
}
async fn me(
    State(shared): State<Shared>,
    Extension(connection): Extension<Connection>,
    request: Request,
) -> Response {
    let grant = match begin(&shared, &request) {
        Ok(g) => g,
        Err(e) => return error(e),
    };
    let token = match session(request.headers()) {
        Ok(t) => t,
        Err(e) => return error(e),
    };
    dispatch(&shared, connection, grant, Operation::Me(token)).await
}
async fn logout(
    State(shared): State<Shared>,
    Extension(connection): Extension<Connection>,
    request: Request,
) -> Response {
    let grant = match begin(&shared, &request) {
        Ok(g) => g,
        Err(e) => return error(e),
    };
    let token = match session(request.headers()) {
        Ok(t) => t,
        Err(e) => return error(e),
    };
    // An automatically sent cookie alone is never CSRF proof. The domain layer
    // compares this explicit token with the hash belonging to this exact session.
    let csrf = match single(request.headers(), "x-csrf-token")
        .and_then(|v| BrowserSecret::parse_hex(v).ok())
    {
        Some(token) => token,
        None => return error(StatusCode::FORBIDDEN),
    };
    dispatch(
        &shared,
        connection,
        grant,
        Operation::Logout {
            session: token,
            csrf,
        },
    )
    .await
}
fn session(headers: &HeaderMap) -> Result<BrowserSecret, StatusCode> {
    let cookie = single(headers, "cookie")
        .and_then(|b| std::str::from_utf8(b).ok())
        .ok_or(StatusCode::UNAUTHORIZED)?;
    let mut found = None;
    for pair in cookie.split(';') {
        let Some((name, value)) = pair.trim().split_once('=') else {
            continue;
        };
        if name == SESSION_COOKIE {
            if found.is_some() {
                return Err(StatusCode::UNAUTHORIZED);
            }
            found = Some(
                BrowserSecret::parse_hex(value.as_bytes()).map_err(|_| StatusCode::UNAUTHORIZED)?,
            );
        }
    }
    found.ok_or(StatusCode::UNAUTHORIZED)
}
async fn dispatch(
    shared: &Shared,
    connection: Connection,
    grant: OwnedPermit,
    operation: Operation,
) -> Response {
    if Instant::now() >= connection.deadline {
        return error(StatusCode::SERVICE_UNAVAILABLE);
    }
    let (reply, receive) = oneshot::channel();
    let command = Command {
        operation,
        completion: Completion {
            deadline: connection.deadline,
            reply,
            request_grant: grant,
        },
    };
    if shared.commands.try_send(command).is_err() {
        return error(StatusCode::TOO_MANY_REQUESTS);
    }
    match receive.await {
        Ok(Ok(Reply::RelayEnrollment(receipt))) => protect((
            if receipt.created { StatusCode::ACCEPTED } else { StatusCode::OK },
            Json(json!({
                "format":"bigbrotr-relay-enrollment/1", "command_key":receipt.command_key,
                "endpoint_id":receipt.endpoint_id, "access_scope_id":receipt.access_scope_id,
                "route_id":receipt.route_id, "enabled":receipt.enabled,
                "generation":receipt.generation, "accepted_at_us":receipt.accepted_at_us.to_string(),
                "created":receipt.created, "application":"unobserved", "authority":"acceptance-only"
            })),
        ).into_response()),
        Ok(Ok(Reply::ResumeExport(receipt))) => protect((
            if receipt.created { StatusCode::ACCEPTED } else { StatusCode::OK },
            Json(json!({"format":"bigbrotr-export-resume/1", "command_key":receipt.command_key,
                "job_id":receipt.job_id, "job_revision":receipt.job_revision,
                "accepted_at_us":receipt.accepted_at_us.to_string(), "created":receipt.created,
                "execution":"unobserved", "authority":"acceptance-only"}))
        ).into_response()),
        Ok(Ok(Reply::JobStatus(Some(value)))) => protect(Json(value).into_response()),
        Ok(Ok(Reply::JobStatus(None))) => failure(StatusCode::NOT_FOUND, "job_unavailable", "Job unavailable to this account.", "check_state"),
        Ok(Ok(Reply::Job(receipt))) => protect((
            if receipt.created { StatusCode::ACCEPTED } else { StatusCode::OK },
            Json(json!({"format":"bigbrotr-job-acceptance/1", "command_key":receipt.command_key,
                "job_id":receipt.job_id, "accepted_at_us":receipt.accepted_at_us.to_string(),
                "created":receipt.created, "execution":"unobserved", "authority":"acceptance-only"}))
        ).into_response()),
        Ok(Ok(Reply::JobList(body))) => {
            let mut response = Response::new(Body::from(axum::body::Bytes::from_owner(body)));
            response.headers_mut().insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
            protect(response)
        },
        Ok(Ok(Reply::History(body))) => {
            let mut response = Response::new(Body::from(axum::body::Bytes::from_owner(*body)));
            response.headers_mut().insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
            protect(response)
        },
        Ok(Ok(Reply::Grant(receipt))) => protect(Json(json!({
            "format":"bigbrotr-scope-grant/1", "command_key":receipt.command_key,
            "principal_id":receipt.principal_id, "scope":receipt.scope,
            "enabled":receipt.enabled, "scope_generation":receipt.scope_generation,
            "account_grant_revision":receipt.account_grant_revision,
            "accepted_at_us":receipt.accepted_at_us.to_string(), "created":receipt.created,
            "application":"unobserved", "authority":"acceptance-only"
        })).into_response()),
        Ok(Ok(Reply::Reauthenticated(proof))) => protect((StatusCode::CREATED, Json(json!({
            "format": "bigbrotr-reauthentication/1", "proof_id": proof.id,
            "command_key": proof.command_key, "command_digest": proof.command_digest,
            "expires_at_us": proof.expires_at_us.to_string(), "authority": "same-session-command-only"
        }))).into_response()),
        Ok(Ok(Reply::Login(issued))) => {
            let mut response =
                (StatusCode::CREATED, Json(observation(issued.observation))).into_response();
            cookie(
                &mut response,
                SESSION_COOKIE,
                &issued.session.expose_hex(),
                true,
                false,
            );
            cookie(
                &mut response,
                CSRF_COOKIE,
                &issued.csrf.expose_hex(),
                false,
                false,
            );
            protect(response)
        }
        Ok(Ok(Reply::Me(value))) => protect(Json(observation(value)).into_response()),
        Ok(Ok(Reply::Logout)) => {
            let mut response = StatusCode::NO_CONTENT.into_response();
            cookie(&mut response, SESSION_COOKIE, "", true, true);
            cookie(&mut response, CSRF_COOKIE, "", false, true);
            protect(response)
        }
        Ok(Err(SessionError::Denied)) => error(StatusCode::UNAUTHORIZED),
        Ok(Err(SessionError::Forbidden)) => failure(StatusCode::FORBIDDEN, "permission_denied", "This action is not permitted.", "check_state"),
        Ok(Err(SessionError::Conflict)) => error(StatusCode::CONFLICT),
        Ok(Err(SessionError::Throttled | SessionError::Capacity)) => {
            error(StatusCode::TOO_MANY_REQUESTS)
        }
        Ok(Err(SessionError::InvalidInput)) => error(StatusCode::BAD_REQUEST),
        Ok(Err(SessionError::CursorInvalid)) => failure(StatusCode::BAD_REQUEST, "invalid_cursor", "Continuation token refused.", "correct_request"),
        Ok(Err(SessionError::CursorExpired)) => failure(StatusCode::GONE, "cursor_expired", "Continuation expired. Start a new listing.", "check_state"),
        Ok(Err(SessionError::CursorIncompatible)) => failure(StatusCode::CONFLICT, "cursor_incompatible", "Continuation no longer matches this request or authority.", "check_state"),
        _ => error(StatusCode::SERVICE_UNAVAILABLE),
    }
}
fn observation(value: SessionObservation) -> serde_json::Value {
    json!({"format":"bigbrotr-session/1", "principal_id":value.principal.to_string(),
        "credential_revision":value.credential_revision.to_string(), "grant_revision":value.grant_revision.to_string(),
        "config_revision":value.config_revision.to_string(), "policy_revision":value.policy_revision.to_string(), "expires_at_us":value.expires_at_us.to_string(),
        "idle_expires_at_us":value.idle_expires_at_us.to_string(), "stored_actions":value.stored_actions,
        "authority":"observation-only"})
}
fn cookie(response: &mut Response, name: &str, value: &str, http_only: bool, clear: bool) {
    let lifetime = if clear { 0 } else { 28_800 };
    let text = format!(
        "{name}={value}; Path=/; Max-Age={lifetime}; SameSite=Strict{}",
        if http_only { "; HttpOnly" } else { "" }
    );
    response.headers_mut().append(
        header::SET_COOKIE,
        HeaderValue::from_str(&text).expect("closed cookie alphabet"),
    );
}
fn protect(mut response: Response) -> Response {
    for (name, value) in [
        ("cache-control", "no-store"),
        ("x-content-type-options", "nosniff"),
        ("x-frame-options", "DENY"),
        (
            "content-security-policy",
            "default-src 'none'; frame-ancestors 'none'",
        ),
    ] {
        response
            .headers_mut()
            .insert(name, HeaderValue::from_static(value));
    }
    response
}
fn error(status: StatusCode) -> Response {
    let (code, message, retry) = match status {
        StatusCode::BAD_REQUEST => ("invalid_request", "Invalid request.", "correct_request"),
        StatusCode::CONFLICT => (
            "command_conflict",
            "Command key or expected revision conflicts with current state.",
            "check_state",
        ),
        StatusCode::UNAUTHORIZED => (
            "authentication_denied",
            "Authentication required or denied.",
            "authenticate",
        ),
        StatusCode::FORBIDDEN => (
            "browser_boundary_denied",
            "Browser request boundary refused.",
            "correct_request",
        ),
        StatusCode::PAYLOAD_TOO_LARGE => (
            "request_too_large",
            "Request exceeds its byte limit.",
            "correct_request",
        ),
        StatusCode::UNSUPPORTED_MEDIA_TYPE => (
            "unsupported_media_type",
            "JSON content type required.",
            "correct_request",
        ),
        StatusCode::TOO_MANY_REQUESTS => (
            "capacity_or_rate_limited",
            "Request capacity or attempt limit reached.",
            "retry_later",
        ),
        _ => (
            "authority_unavailable",
            "Current authority or dependency unavailable.",
            "check_state",
        ),
    };
    failure(status, code, message, retry)
}
fn failure(status: StatusCode, code: &str, message: &str, retry: &str) -> Response {
    let correlation = bigbrotr::runtime::ownership::fresh_process_incarnation()
        .ok()
        .map(|id| id.to_string());
    let mut response = protect(
        (
            status,
            Json(json!({"error":{"status":status.as_u16(),
        "code":code,"message":message,"retry_class":retry,"correlation_id":correlation}})),
        )
            .into_response(),
    );
    if status == StatusCode::TOO_MANY_REQUESTS {
        response
            .headers_mut()
            .insert(header::RETRY_AFTER, HeaderValue::from_static("60"));
    }
    response
}

#[cfg(test)]
mod terminal_tests {
    use super::*;
    #[tokio::test]
    async fn only_positive_http_completion_or_all_joined_releases_delivery() {
        for normal in [true, false] {
            let (end, terminal) = watch::channel(false);
            let (all, joined) = watch::channel(false);
            let mut delivery = ConnectionDelivery {
                action: ActionGate::new(),
                terminal,
                joined,
            };
            if normal {
                end.send_replace(true);
            }
            drop(end);
            if normal {
                tokio::time::timeout(Duration::from_secs(1), delivery.ended())
                    .await
                    .unwrap();
            } else {
                assert!(
                    tokio::time::timeout(Duration::from_millis(20), delivery.ended())
                        .await
                        .is_err()
                );
                all.send_replace(true);
                drop(all);
                tokio::time::timeout(Duration::from_secs(1), delivery.ended())
                    .await
                    .unwrap();
            }
        }
        // Exercise notification after the waiter actually began, followed by
        // sender destruction. The final true must survive channel closure.
        let (end, terminal) = watch::channel(false);
        let (_all, joined) = watch::channel(false);
        let mut delivery = ConnectionDelivery {
            action: ActionGate::new(),
            terminal,
            joined,
        };
        let waiter = tokio::spawn(async move { delivery.ended().await });
        tokio::task::yield_now().await;
        end.send_replace(true);
        drop(end);
        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .unwrap()
            .unwrap();
    }
}
