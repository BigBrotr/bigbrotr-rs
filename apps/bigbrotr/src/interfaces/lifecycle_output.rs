//! Process-owned diagnostics for `serve`, never a durable receipt or data worker.
use std::{
    io::{self, Write},
    os::fd::OwnedFd,
    sync::mpsc::{self, Receiver, SyncSender},
    thread,
    time::Duration,
};

const LINE_BYTES: usize = 4096;
const DRAIN: Duration = Duration::from_millis(200);

struct Line {
    bytes: [u8; LINE_BYTES],
    used: usize,
    error: bool,
}
impl Write for Line {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let end = self
            .used
            .checked_add(bytes.len())
            .filter(|&end| end <= LINE_BYTES)
            .ok_or_else(|| io::Error::other("lifecycle output limit"))?;
        self.bytes[self.used..end].copy_from_slice(bytes);
        self.used = end;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub(super) struct LifecycleOutput {
    sender: SyncSender<Line>,
    done: Receiver<bool>,
    queued: bool,
}
impl LifecycleOutput {
    pub(super) fn start() -> io::Result<Self> {
        // No stdio locks/buffers: Rust's final stdout cleanup must never wait
        // behind this writer. Descriptor flags remain unchanged for the parent.
        let stdout = rustix::io::fcntl_dupfd_cloexec(std::io::stdout(), 0)?;
        let stderr = rustix::io::fcntl_dupfd_cloexec(std::io::stderr(), 0)?;
        let (sender, receiver) = mpsc::sync_channel::<Line>(2);
        let (finished, done) = mpsc::sync_channel(1);
        thread::Builder::new()
            .name("lifecycle-output".into())
            .stack_size(128 * 1024)
            .spawn(move || {
                let mut success = true;
                for line in receiver {
                    let fd = if line.error { &stderr } else { &stdout };
                    success &= write_line(fd, &line.bytes[..line.used]).is_ok();
                }
                let _ = finished.send(success);
            })?;
        Ok(Self {
            sender,
            done,
            queued: true,
        })
    }

    pub(super) fn emit(&mut self, value: impl std::fmt::Display, error: bool) {
        let mut line = Line {
            bytes: [0; LINE_BYTES],
            used: 0,
            error,
        };
        if writeln!(&mut line, "{value}").is_err() || self.sender.try_send(line).is_err() {
            self.queued = false;
        }
    }

    /// Called only at the CLI's process-exit boundary, after every data owner and
    /// driver is terminal. A blocked diagnostic writer owns only three bounded
    /// lines, its stack and these descriptors. The caller MUST exit the process
    /// next: dropping this handle alone does not cancel a blocking system call.
    pub(super) fn finish(self) -> bool {
        drop(self.sender);
        self.done.recv_timeout(DRAIN).unwrap_or(false) && self.queued
    }
}

fn write_line(fd: &OwnedFd, mut bytes: &[u8]) -> io::Result<()> {
    while !bytes.is_empty() {
        match rustix::io::write(fd, bytes) {
            Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
            Ok(n) => bytes = &bytes[n..],
            Err(rustix::io::Errno::INTR) => continue,
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}
