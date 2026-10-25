//! Local Control driver custody. The supervisor still owns its actual join.
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::sync::oneshot;

// Field order is part of cancellation safety: an unpolled/aborted transport
// must be destroyed before a queued or already received resource keeper.
pub(super) struct RetainedDriver<F: Future, T> {
    connection: Option<F>,
    receiver: oneshot::Receiver<T>,
    keeper: Option<T>,
    result: Option<F::Output>,
    received: bool,
}
impl<F: Future, T> RetainedDriver<F, T> {
    pub(super) fn new(connection: F, receiver: oneshot::Receiver<T>) -> Self {
        Self {
            connection: Some(connection),
            receiver,
            keeper: None,
            result: None,
            received: false,
        }
    }
}
impl<F: Future + Unpin, T: Unpin> Future for RetainedDriver<F, T>
where
    F::Output: Unpin,
{
    type Output = F::Output;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if let Some(connection) = &mut this.connection
            && let Poll::Ready(result) = Pin::new(connection).poll(cx)
        {
            this.result = Some(result);
            // Destroy the actual completed transport before releasing custody.
            this.connection = None;
        }
        if !this.received
            && let Poll::Ready(result) = Pin::new(&mut this.receiver).poll(cx)
        {
            this.keeper = result.ok();
            this.received = true;
        }
        if this.connection.is_none() && this.received {
            Poll::Ready(this.result.take().expect("completed Control connection"))
        } else {
            Poll::Pending
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    };

    struct DropRecord(&'static str, Arc<Mutex<Vec<&'static str>>>);
    impl Drop for DropRecord {
        fn drop(&mut self) {
            self.1.lock().unwrap().push(self.0);
        }
    }
    struct Connection {
        ready: Arc<AtomicBool>,
        _drop: DropRecord,
    }
    impl Future for Connection {
        type Output = ();
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
            if self.ready.load(Ordering::Relaxed) {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        }
    }
    fn poll<F: Future + Unpin>(value: &mut F) -> Poll<F::Output> {
        Pin::new(value).poll(&mut Context::from_waker(std::task::Waker::noop()))
    }

    #[test]
    fn retained_driver_destroys_transport_before_queued_or_received_keeper() {
        for receive in [false, true] {
            let log = Arc::new(Mutex::new(Vec::new()));
            let (sender, receiver) = oneshot::channel();
            let mut driver = RetainedDriver::new(
                Connection {
                    ready: Arc::new(AtomicBool::new(false)),
                    _drop: DropRecord("transport", log.clone()),
                },
                receiver,
            );
            assert!(sender.send(DropRecord("keeper", log.clone())).is_ok());
            if receive {
                assert!(poll(&mut driver).is_pending());
            }
            assert!(log.lock().unwrap().is_empty());
            // Same destruction path as a joined abort, including an unpolled
            // future whose keeper is still queued in the receiver.
            drop(driver);
            assert_eq!(*log.lock().unwrap(), ["transport", "keeper"]);
        }
    }

    #[test]
    fn retained_driver_waits_for_owner_after_early_connection_completion() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let (sender, receiver) = oneshot::channel();
        let mut driver = RetainedDriver::new(
            Connection {
                ready: Arc::new(AtomicBool::new(true)),
                _drop: DropRecord("transport", log.clone()),
            },
            receiver,
        );
        assert!(poll(&mut driver).is_pending());
        assert_eq!(*log.lock().unwrap(), ["transport"]);
        assert!(sender.send(DropRecord("keeper", log.clone())).is_ok());
        assert!(poll(&mut driver).is_ready());
        assert_eq!(*log.lock().unwrap(), ["transport"]);
        drop(driver);
        assert_eq!(*log.lock().unwrap(), ["transport", "keeper"]);
    }

    #[test]
    fn retained_driver_sender_failure_and_closed_sender_do_not_skip_transport() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let (sender, receiver) = oneshot::channel();
        let ready = Arc::new(AtomicBool::new(false));
        let mut driver = RetainedDriver::new(
            Connection {
                ready: ready.clone(),
                _drop: DropRecord("transport", log.clone()),
            },
            receiver,
        );
        assert!(poll(&mut driver).is_pending());
        drop(driver);
        let returned = sender
            .send(DropRecord("keeper", log.clone()))
            .err()
            .unwrap();
        assert_eq!(*log.lock().unwrap(), ["transport"]);
        drop(returned);
        assert_eq!(*log.lock().unwrap(), ["transport", "keeper"]);

        let (sender, receiver) = oneshot::channel::<DropRecord>();
        let mut driver = RetainedDriver::new(
            Connection {
                ready: ready.clone(),
                _drop: DropRecord("second transport", log.clone()),
            },
            receiver,
        );
        drop(sender);
        assert!(poll(&mut driver).is_pending());
        ready.store(true, Ordering::Relaxed);
        assert!(poll(&mut driver).is_ready());
        drop(driver);
        assert_eq!(
            *log.lock().unwrap(),
            ["transport", "keeper", "second transport"]
        );
    }

    #[tokio::test]
    async fn retained_driver_abort_join_confirms_transport_before_keeper_release() {
        struct ObservedConnection {
            inner: Connection,
            polled: Option<oneshot::Sender<()>>,
        }
        impl Future for ObservedConnection {
            type Output = ();
            fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
                let this = self.get_mut();
                if let Some(sender) = this.polled.take() {
                    let _ = sender.send(());
                }
                Pin::new(&mut this.inner).poll(cx)
            }
        }
        for received in [false, true] {
            let log = Arc::new(Mutex::new(Vec::new()));
            let (sender, receiver) = oneshot::channel();
            let (polled, observed) = oneshot::channel();
            assert!(sender.send(DropRecord("keeper", log.clone())).is_ok());
            let task = tokio::spawn(RetainedDriver::new(
                ObservedConnection {
                    inner: Connection {
                        ready: Arc::new(AtomicBool::new(false)),
                        _drop: DropRecord("transport", log.clone()),
                    },
                    polled: Some(polled),
                },
                receiver,
            ));
            if received {
                // The task's synchronous poll receives the keeper before it
                // yields Pending and lets this current-thread test resume.
                tokio::time::timeout(std::time::Duration::from_secs(1), observed)
                    .await
                    .unwrap()
                    .unwrap();
            }
            assert!(log.lock().unwrap().is_empty());
            task.abort();
            let terminal = tokio::time::timeout(std::time::Duration::from_secs(1), task)
                .await
                .unwrap();
            assert!(terminal.unwrap_err().is_cancelled());
            assert_eq!(*log.lock().unwrap(), ["transport", "keeper"]);
        }
    }
}
