//! Lifecycle controls use the actual close-only flock and ResourceBudget. The
//! private retained-descriptor entrypoint isolates executor behavior from the
//! separately tested host/journal identity qualification; it grants no CH proof.
use super::*;
use crate::{
    runtime::{ProcessEnvelope, ResourceBudget, ResourceLimits, ResourceRequest, WaiterLimits},
    storage::custody::{CustodyError, acquire},
};
use std::{
    cell::RefCell,
    fs::OpenOptions,
    os::unix::fs::OpenOptionsExt,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{self, Receiver, SyncSender},
    },
    time::Duration,
};

const WAIT: Duration = Duration::from_secs(5);

fn reservation() -> (ResourceBudget, OwnedPermit) {
    let budget = ResourceBudget::new(
        ProcessEnvelope::new(
            INDEX_LANE_STACK_RESERVATION + 4096,
            0,
            ResourceLimits::new(1024, 1, INDEX_LANE_STACK_RESERVATION, 64),
            WaiterLimits::new(1, 1),
        )
        .unwrap(),
    );
    let permit = budget
        .try_acquire(ResourceRequest::data(1024, 1, INDEX_LANE_STACK_RESERVATION).unwrap())
        .unwrap();
    (budget, permit)
}
fn lock() -> (tempfile::TempDir, File, File) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("custody.lock");
    let held = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&path)
        .unwrap();
    acquire(&held).unwrap();
    let peer = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    (directory, held, peer)
}
fn start<T, F, Work>(file: File, permit: OwnedPermit, factory: F) -> OwnedIndexLane<T>
where
    T: Send + 'static,
    F: FnOnce() -> Work + Send + 'static,
    Work: Future<Output = T> + 'static,
{
    OwnedIndexLane::start_retained(
        file,
        permit,
        Instant::now() + Duration::from_secs(30),
        factory,
        |job| {
            thread::Builder::new()
                .stack_size(THREAD_STACK_BYTES)
                .spawn(job)
        },
    )
    .unwrap()
}
fn retained(budget: &ResourceBudget, peer: &File) {
    assert_eq!(budget.snapshot().active_permits(), 1);
    assert_eq!(acquire(peer), Err(CustodyError::Occupied));
}
fn released(budget: &ResourceBudget, peer: &File) {
    assert_eq!(budget.snapshot().active_permits(), 0);
    // Concurrent test forks may briefly inherit CLOEXEC file descriptions.
    // The result must be actual reacquisition, never elapsed time as evidence.
    let deadline = Instant::now() + WAIT;
    loop {
        match acquire(peer) {
            Err(CustodyError::Occupied) if Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(2))
            }
            result => {
                result.unwrap();
                break;
            }
        }
    }
}
struct SignalDrop(SyncSender<()>);
impl Drop for SignalDrop {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}
struct BarrierDrop {
    entered: SyncSender<()>,
    release: Receiver<()>,
}
impl Drop for BarrierDrop {
    fn drop(&mut self) {
        self.entered.send(()).unwrap();
        self.release
            .recv_timeout(WAIT)
            .expect("test must release teardown barrier");
    }
}

#[test]
fn completed_result_follows_hidden_task_drop_and_retains_its_grant() {
    let (_directory, held, peer) = lock();
    let (budget, permit) = reservation();
    let (dropped, observe_drop) = mpsc::sync_channel(1);
    let lane = start(held, permit, move || async move {
        let (ready, observe_ready) = tokio::sync::oneshot::channel();
        drop(tokio::spawn(async move {
            let _drop = SignalDrop(dropped);
            ready.send(()).unwrap();
            std::future::pending::<()>().await;
        }));
        observe_ready.await.unwrap();
        vec![7u8; 32]
    });
    let joined = lane.join().unwrap();
    observe_drop.recv_timeout(WAIT).unwrap();
    retained(&budget, &peer);
    assert!(matches!(joined.outcome(),LaneOutcome::Completed(v) if v==&vec![7;32]));
    let (outcome, permit) = joined.into_parts();
    assert_eq!(budget.snapshot().active_permits(), 1);
    drop(outcome);
    drop(permit);
    released(&budget, &peer);
}

#[test]
fn cancellation_keeps_custody_and_grant_while_hidden_async_task_is_destroyed() {
    let (_directory, held, peer) = lock();
    let (budget, permit) = reservation();
    let (ready, observe_ready) = mpsc::sync_channel(1);
    let (teardown, observe_teardown) = mpsc::sync_channel(1);
    let (release, wait_release) = mpsc::sync_channel(1);
    let mut lane = start(held, permit, move || async move {
        let (installed, observe_installed) = tokio::sync::oneshot::channel();
        drop(tokio::spawn(async move {
            let _barrier = BarrierDrop {
                entered: teardown,
                release: wait_release,
            };
            installed.send(()).unwrap();
            std::future::pending::<()>().await;
        }));
        observe_installed.await.unwrap();
        ready.send(()).unwrap();
        std::future::pending::<()>().await;
    });
    observe_ready.recv_timeout(WAIT).unwrap();
    lane.request_cancel();
    observe_teardown.recv_timeout(WAIT).unwrap();
    retained(&budget, &peer);
    assert!(lane.try_join().unwrap().is_none());
    release.send(()).unwrap();
    let joined = lane.join().unwrap();
    assert!(matches!(joined.outcome(), LaneOutcome::Cancelled));
    retained(&budget, &peer);
    drop(joined);
    released(&budget, &peer);
}

#[test]
fn cancelling_staged_response_joins_the_hidden_http_connection_before_release() {
    use hyper_util::client::legacy::connect::HttpConnector;
    use std::{
        io::{Read, Write},
        net::TcpListener,
        num::NonZeroUsize,
    };

    let (_directory, held, peer) = lock();
    let local = crate::storage::clickhouse::adapter::PhysicalLimits::new(
        NonZeroUsize::new(64).unwrap(),
        8192,
        1,
        1024 * 1024,
        Duration::from_secs(1),
    )
    .unwrap()
    .required_local_data_bytes()
    .unwrap();
    let budget = ResourceBudget::new(
        ProcessEnvelope::new(
            local + INDEX_LANE_STACK_RESERVATION + 64,
            0,
            ResourceLimits::new(local, 1, INDEX_LANE_STACK_RESERVATION, 64),
            WaiterLimits::new(0, 0),
        )
        .unwrap(),
    );
    let permit = budget
        .try_acquire(ResourceRequest::data(local, 1, INDEX_LANE_STACK_RESERVATION).unwrap())
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let address = listener.local_addr().unwrap();
    let (sent, observe_sent) = mpsc::sync_channel(1);
    let (closed, observe_closed) = mpsc::sync_channel(1);
    let server = thread::spawn(move || {
        let deadline = Instant::now() + WAIT;
        let (mut socket, _) = loop {
            match listener.accept() {
                Err(error)
                    if error.kind() == io::ErrorKind::WouldBlock && Instant::now() < deadline =>
                {
                    thread::sleep(Duration::from_millis(2));
                }
                result => break result.unwrap(),
            }
        };
        socket.set_nonblocking(false).unwrap();
        socket.set_read_timeout(Some(WAIT)).unwrap();
        socket.set_write_timeout(Some(WAIT)).unwrap();
        let mut header = Vec::new();
        while !header.ends_with(b"\r\n\r\n") {
            assert!(header.len() < 8192);
            let mut byte = [0];
            socket.read_exact(&mut byte).unwrap();
            header.extend_from_slice(&byte);
        }
        let length: usize = std::str::from_utf8(&header)
            .unwrap()
            .lines()
            .filter_map(|line| line.split_once(':'))
            .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
            .unwrap()
            .1
            .trim()
            .parse()
            .unwrap();
        assert!(length <= 8192);
        socket.read_exact(&mut vec![0; length]).unwrap();
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n1\n")
            .unwrap();
        sent.send(()).unwrap();
        // Keep the peer's write side open. Only complete local cancellation
        // teardown can make this read observe the client's actual socket close.
        let mut byte = [0];
        match socket.read(&mut byte) {
            Ok(0) => {}
            Err(error) if error.kind() == io::ErrorKind::ConnectionReset => {}
            result => panic!("cancelled client socket did not close: {result:?}"),
        }
        closed.send(()).unwrap();
    });
    let lane = start(held, permit, move || async move {
        let client = crate::storage::clickhouse::transport::bounded_client(
            HttpConnector::new(),
            NonZeroUsize::new(64).unwrap(),
        )
        .with_url(format!("http://{address}"));
        client.query("SELECT 1").execute().await
    });
    observe_sent.recv_timeout(WAIT).unwrap();
    retained(&budget, &peer);
    lane.request_cancel();
    let joined = lane.join().unwrap();
    assert!(matches!(joined.outcome(), LaneOutcome::Cancelled));
    observe_closed.recv_timeout(WAIT).unwrap();
    server.join().unwrap();
    retained(&budget, &peer);
    drop(joined);
    released(&budget, &peer);
}

#[test]
fn cancelled_blocking_task_is_joined_instead_of_leaked_by_runtime_timeout() {
    let (_directory, held, peer) = lock();
    let (budget, permit) = reservation();
    let (ready, observe_ready) = mpsc::sync_channel(1);
    let (future_dropped, observe_future_drop) = mpsc::sync_channel(1);
    let (release, wait_release) = mpsc::sync_channel(1);
    let mut lane = start(held, permit, move || async move {
        let _drop = SignalDrop(future_dropped);
        let (started, observe_started) = tokio::sync::oneshot::channel();
        drop(tokio::task::spawn_blocking(move || {
            started.send(()).unwrap();
            wait_release.recv_timeout(WAIT).unwrap();
        }));
        observe_started.await.unwrap();
        ready.send(()).unwrap();
        std::future::pending::<()>().await;
    });
    observe_ready.recv_timeout(WAIT).unwrap();
    lane.request_cancel();
    observe_future_drop.recv_timeout(WAIT).unwrap();
    retained(&budget, &peer);
    assert!(lane.try_join().unwrap().is_none());
    release.send(()).unwrap();
    let joined = lane.join().unwrap();
    assert!(matches!(joined.outcome(), LaneOutcome::Cancelled));
    drop(joined);
    released(&budget, &peer);
}

#[test]
fn dropping_the_owner_cancels_and_joins_before_releasing_capacity() {
    let (_directory, held, peer) = lock();
    let (budget, permit) = reservation();
    let (ready, observe_ready) = mpsc::sync_channel(1);
    let (teardown, observe_teardown) = mpsc::sync_channel(1);
    let (release, wait_release) = mpsc::sync_channel(1);
    let lane = start(held, permit, move || async move {
        let _barrier = BarrierDrop {
            entered: teardown,
            release: wait_release,
        };
        ready.send(()).unwrap();
        std::future::pending::<()>().await;
    });
    observe_ready.recv_timeout(WAIT).unwrap();
    let dropper = thread::spawn(move || drop(lane));
    observe_teardown.recv_timeout(WAIT).unwrap();
    assert!(!dropper.is_finished());
    retained(&budget, &peer);
    release.send(()).unwrap();
    dropper.join().unwrap();
    released(&budget, &peer);
}

thread_local! {
    static THREAD_EXIT: RefCell<Option<BarrierDrop>> = const { RefCell::new(None) };
}
#[test]
fn resources_survive_executor_drop_until_the_actual_os_thread_join() {
    let (_directory, held, peer) = lock();
    let (budget, permit) = reservation();
    let (thread_exit, observe_thread_exit) = mpsc::sync_channel(1);
    let (release, wait_release) = mpsc::sync_channel(1);
    let lane = start(held, permit, move || async move {
        THREAD_EXIT.with(|state| {
            *state.borrow_mut() = Some(BarrierDrop {
                entered: thread_exit,
                release: wait_release,
            })
        });
        11
    });
    let joiner = thread::spawn(move || lane.join().unwrap());
    // This barrier runs after the worker function and complete Runtime Drop.
    observe_thread_exit.recv_timeout(WAIT).unwrap();
    assert!(!joiner.is_finished());
    retained(&budget, &peer);
    release.send(()).unwrap();
    let joined = joiner.join().unwrap();
    assert!(matches!(joined.outcome(), LaneOutcome::Completed(11)));
    retained(&budget, &peer);
    drop(joined);
    released(&budget, &peer);
}

#[test]
fn panic_unwinds_runtime_before_returning_a_joined_failure() {
    let (_directory, held, peer) = lock();
    let (budget, permit) = reservation();
    let (dropped, observe_drop) = mpsc::sync_channel(1);
    let lane = start(held, permit, move || async move {
        let (ready, observe_ready) = tokio::sync::oneshot::channel();
        drop(tokio::spawn(async move {
            let _drop = SignalDrop(dropped);
            ready.send(()).unwrap();
            std::future::pending::<()>().await;
        }));
        observe_ready.await.unwrap();
        panic!("synthetic Index operation panic");
    });
    let joined = lane.join().unwrap();
    observe_drop.recv_timeout(WAIT).unwrap();
    assert!(matches!(joined.outcome(), LaneOutcome::Panicked));
    retained(&budget, &peer);
    drop(joined);
    released(&budget, &peer);
}

#[test]
fn expired_dispatch_never_invokes_the_factory() {
    let (_directory, held, peer) = lock();
    let (budget, permit) = reservation();
    let invoked = Arc::new(AtomicBool::new(false));
    let observed = invoked.clone();
    let lane = OwnedIndexLane::start_retained(
        held,
        permit,
        Instant::now(),
        move || {
            observed.store(true, Ordering::SeqCst);
            async {}
        },
        |job| {
            thread::Builder::new()
                .stack_size(THREAD_STACK_BYTES)
                .spawn(job)
        },
    )
    .unwrap();
    let joined = lane.join().unwrap();
    assert!(matches!(joined.outcome(), LaneOutcome::Deadline));
    assert!(!invoked.load(Ordering::SeqCst));
    retained(&budget, &peer);
    drop(joined);
    released(&budget, &peer);
}

#[test]
fn failed_thread_construction_releases_inputs_without_dispatch() {
    let (_directory, held, peer) = lock();
    let (budget, permit) = reservation();
    let invoked = Arc::new(AtomicBool::new(false));
    let observed = invoked.clone();
    let result = OwnedIndexLane::start_retained(
        held,
        permit,
        Instant::now() + WAIT,
        move || {
            observed.store(true, Ordering::SeqCst);
            async {}
        },
        |_job| Err(io::Error::other("synthetic thread construction failure")),
    );
    assert_eq!(result.err(), Some(LaneError::ThreadUnavailable));
    assert!(!invoked.load(Ordering::SeqCst));
    released(&budget, &peer);
}

#[test]
fn a_control_or_unbudgeted_data_grant_cannot_start_the_lane() {
    for control in [false, true] {
        let (_directory, held, peer) = lock();
        let (budget, full) = reservation();
        drop(full);
        let request = if control {
            ResourceRequest::control(64)
        } else {
            ResourceRequest::data(1, 1, 0)
        }
        .unwrap();
        let permit = budget.try_acquire(request).unwrap();
        let result = OwnedIndexLane::start_retained(
            held,
            permit,
            Instant::now() + WAIT,
            || async {},
            |_job| panic!("invalid capacity must fail before thread construction"),
        );
        assert_eq!(result.err(), Some(LaneError::InvalidGrant));
        released(&budget, &peer);
    }
}
