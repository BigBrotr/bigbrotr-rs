//! One explicit nonblocking connect boundary for resolver and relay sockets.
//! Submission proof is captured before Tokio registration/readiness can fail.
use super::{
    lifetime::Gate,
    observation::{
        ConnectErrorClass, ConnectFailureStage, ConnectSubmission, FirstIoTarget, TransportTrace,
    },
};
use socket2::{Domain, Protocol, SockAddr, Socket, Type};
use std::{io, net::SocketAddr};
use tokio::net::TcpStream;

fn report(
    trace: Option<&TransportTrace>,
    target: FirstIoTarget,
    stage: ConnectFailureStage,
    error: &io::Error,
) {
    if let Some(trace) = trace {
        trace.connect_failed(target, stage, error);
    }
}

fn accepted(error: &io::Error) -> bool {
    #[cfg(not(windows))]
    {
        error.raw_os_error() == Some(rustix::io::Errno::INPROGRESS.raw_os_error())
    }
    #[cfg(windows)]
    {
        error.kind() == io::ErrorKind::WouldBlock
    }
}

fn submit(
    socket: &Socket,
    address: &SockAddr,
    gate: Option<&Gate<'_>>,
    target: FirstIoTarget,
) -> io::Result<()> {
    let trace = gate.and_then(Gate::observation);
    if let Some(trace) = trace {
        trace.prepare_connect().map_err(io::Error::other)?;
    }
    if let Some(gate) = gate {
        gate.check_io()?;
    }
    // This is the externally-directed syscall. No allocation, await, reactor
    // registration or policy check separates its return from the capture.
    let result = socket.connect(address);
    let submission = match &result {
        Ok(()) => ConnectSubmission::Connected,
        Err(error) if accepted(error) => ConnectSubmission::InProgress,
        Err(error) => ConnectSubmission::ImmediateError(ConnectErrorClass::of(error)),
    };
    if let Some(trace) = trace {
        trace.connect_returned(target, submission);
    }
    match result {
        Ok(()) => Ok(()),
        Err(error) if accepted(&error) => Ok(()),
        Err(error) => {
            report(trace, target, ConnectFailureStage::Immediate, &error);
            Err(error)
        }
    }
}

fn registered(
    result: io::Result<TcpStream>,
    trace: Option<&TransportTrace>,
    target: FirstIoTarget,
) -> io::Result<TcpStream> {
    result.inspect_err(|error| report(trace, target, ConnectFailureStage::Registration, error))
}

pub(super) async fn connect(
    address: SocketAddr,
    gate: Option<&Gate<'_>>,
    target: FirstIoTarget,
) -> io::Result<TcpStream> {
    let trace = gate.and_then(Gate::observation);
    let socket = (|| {
        let socket = Socket::new(
            Domain::for_address(address),
            Type::STREAM,
            Some(Protocol::TCP),
        )?;
        socket.set_nonblocking(true)?;
        socket.set_tcp_nodelay(true)?;
        Ok::<_, io::Error>(socket)
    })()
    .inspect_err(|error| report(trace, target, ConnectFailureStage::Preparation, error))?;
    let selected = SockAddr::from(address);
    submit(&socket, &selected, gate, target)?;
    if let Some(gate) = gate {
        gate.check_io()?;
    }
    let socket = registered(TcpStream::from_std(socket.into()), trace, target)?;
    if let Some(gate) = gate {
        gate.check_io()?;
    }
    socket
        .writable()
        .await
        .inspect_err(|error| report(trace, target, ConnectFailureStage::Readiness, error))?;
    if let Some(gate) = gate {
        gate.check_io()?;
    }
    if let Some(error) = socket
        .take_error()
        .inspect_err(|error| report(trace, target, ConnectFailureStage::SocketStatus, error))?
    {
        report(trace, target, ConnectFailureStage::Completion, &error);
        return Err(error);
    }
    if socket
        .peer_addr()
        .inspect_err(|error| report(trace, target, ConnectFailureStage::Peer, error))?
        != address
    {
        let error = io::Error::other("pinned connection peer differs");
        report(trace, target, ConnectFailureStage::Peer, &error);
        return Err(error);
    }
    Ok(socket)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        network::{CancelAttempt, observation::ObservedStop},
        runtime::policy_authority::{LeaseResponse, PolicyLeaseTracker, PolicyLeaseTtl},
    };
    use std::{net::TcpListener, time::Duration};
    use tokio::time::Instant;

    pub(super) fn gate(trace: &TransportTrace) -> (Gate<'static>, CancelAttempt) {
        let dataset = [1; 16].try_into().unwrap();
        let process_incarnation = [2; 16].try_into().unwrap();
        let epoch = bigbrotr_model::PolicyEpoch::new(1);
        let ttl = Duration::from_secs(5);
        let mut tracker = PolicyLeaseTracker::new(
            dataset,
            process_incarnation,
            epoch,
            PolicyLeaseTtl::new(ttl).unwrap(),
        )
        .unwrap();
        let ticket = tracker.begin_refresh().unwrap();
        tracker
            .complete_refresh(
                &ticket,
                LeaseResponse {
                    dataset,
                    process_incarnation,
                    policy_epoch: epoch,
                    ttl,
                },
            )
            .unwrap();
        let window = tracker.begin_action(epoch).unwrap().complete(ttl).unwrap();
        let deadline = Instant::now() + ttl;
        trace.begin(deadline).unwrap();
        let (gate, cancel) = Gate::new(window, deadline);
        (gate.with_observation(trace.clone()), cancel)
    }
    fn socket() -> Socket {
        let socket = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP)).unwrap();
        socket.set_nonblocking(true).unwrap();
        socket
    }

    #[test]
    fn cancelled_before_connect_has_no_external_action_proof() {
        let trace = TransportTrace::new();
        let (gate, cancel) = gate(&trace);
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = SockAddr::from(listener.local_addr().unwrap());
        drop(cancel);
        assert!(
            submit(
                &socket(),
                &address,
                Some(&gate),
                FirstIoTarget::RelayConnect
            )
            .is_err()
        );
        let captured = trace.snapshot();
        assert!(captured.first_io.is_none());
        assert_eq!(captured.relay_connect_calls, 0);
        assert_eq!(captured.stop, Some(ObservedStop::Cancelled));
        assert_eq!(
            listener.accept().unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
    }

    #[test]
    fn actual_immediate_syscall_error_is_distinct_from_no_submission() {
        let trace = TransportTrace::new();
        let (gate, _cancel) = gate(&trace);
        // A deliberately incompatible prepared address reaches the actual
        // syscall and fails immediately. It proves no packet/remote receipt.
        let address = SockAddr::from("[::1]:9".parse::<SocketAddr>().unwrap());
        assert!(
            submit(
                &socket(),
                &address,
                Some(&gate),
                FirstIoTarget::ResolverConnect
            )
            .is_err()
        );
        let captured = trace.snapshot();
        let first = captured.first_io.unwrap();
        assert_eq!(first.target, FirstIoTarget::ResolverConnect);
        assert!(matches!(
            first.submission,
            ConnectSubmission::ImmediateError(_)
        ));
        assert_eq!(captured.resolver_connect_calls, 1);
        assert_eq!(
            captured.last_connect_failure.unwrap().stage,
            ConnectFailureStage::Immediate
        );
    }

    #[test]
    fn accepted_syscall_survives_a_later_local_registration_failure() {
        let trace = TransportTrace::new();
        let (gate, _cancel) = gate(&trace);
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = SockAddr::from(listener.local_addr().unwrap());
        let socket = socket();
        submit(&socket, &address, Some(&gate), FirstIoTarget::RelayConnect).unwrap();
        assert!(
            registered(
                Err(io::Error::other("injected local registration refusal")),
                Some(&trace),
                FirstIoTarget::RelayConnect
            )
            .is_err()
        );
        let captured = trace.snapshot();
        assert!(matches!(
            captured.first_io.unwrap().submission,
            ConnectSubmission::Connected | ConnectSubmission::InProgress
        ));
        assert_eq!(
            captured.last_connect_failure.unwrap().stage,
            ConnectFailureStage::Registration
        );
        assert!(captured.relay_connected.is_none());
    }

    #[test]
    fn prior_clock_refusal_cannot_become_submission_proof() {
        let trace = TransportTrace::new();
        let (gate, _cancel) = gate(&trace);
        trace.stop(ObservedStop::Clock);
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        assert!(
            submit(
                &socket(),
                &SockAddr::from(listener.local_addr().unwrap()),
                Some(&gate),
                FirstIoTarget::RelayConnect
            )
            .is_err()
        );
        assert!(trace.snapshot().first_io.is_none());
        assert_eq!(trace.snapshot().stop, Some(ObservedStop::Clock));
    }

    #[tokio::test]
    async fn tls_construction_and_upper_queued_plaintext_do_not_prove_lower_io() {
        use crate::network::{TlsPolicy, stream::Guarded};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (socket, peer) = tokio::join!(
            TcpStream::connect(listener.local_addr().unwrap()),
            listener.accept()
        );
        let mut peer = peer.unwrap().0;
        let trace = TransportTrace::new();
        let (gate, _cancel) = gate(&trace);
        let phase = trace.tls_phase();
        let policy = TlsPolicy::new(&[]).unwrap();
        let lower = Guarded {
            socket: socket.unwrap(),
            gate: gate.clone(),
            capture_tls_io: true,
        };
        let future = policy.connector.connect_with(
            rustls::pki_types::ServerName::try_from("relay.example").unwrap(),
            lower,
            |session| session.set_buffer_limit(Some(64 * 1024)),
        );
        // ClientConnection was constructed and has a queued ClientHello. The
        // handshake future has not polled the lower transport.
        assert!(trace.snapshot().tls_first_lower_write.is_none());
        assert_eq!(
            peer.try_read(&mut [0; 1]).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        let mut upper = Guarded {
            socket: tokio::io::sink(),
            gate,
            capture_tls_io: false,
        };
        upper
            .write_all(b"queued plaintext accepted above TLS")
            .await
            .unwrap();
        assert!(trace.snapshot().tls_first_lower_write.is_none());
        drop(future);
        drop(phase);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), peer.read(&mut [0; 1]))
                .await
                .unwrap()
                .unwrap(),
            0
        );
    }
}
