//! Synchronous private-worker transport with nonblocking pipes and owned grants.
//! Only trusted workers that do not fork descendants may use this boundary.
use crate::runtime::OwnedPermit;
use rustix::fs::{OFlags, fcntl_getfl, fcntl_setfl};
use std::{
    io::{self, Read, Write},
    os::fd::AsFd,
    process::{Child, ChildStderr, ChildStdout, Command, ExitStatus, Stdio},
    sync::atomic::{AtomicBool, Ordering},
    thread,
    time::{Duration, Instant},
};

const PIPE_BYTES: usize = 4096;
const POLL: Duration = Duration::from_millis(2);
const KILL_WAIT: Duration = Duration::from_millis(300);
pub(super) const CONTROL_BYTES: u64 = 32 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessError {
    InsufficientReservation,
    Spawn,
    Deadline,
    Cancelled,
    OutputLimit,
    Exit,
    Io,
    /// The caller must retain the job root and treat native activity as unknown.
    TerminationUnknown,
}

pub(super) struct Process<'a> {
    child: Child,
    stdout: Capture<ChildStdout>,
    stderr: Capture<ChildStderr>,
    status: Option<ExitStatus>,
    failure: Option<ProcessError>,
    native: Option<OwnedPermit>,
    control: Option<OwnedPermit>,
    tls: Option<super::client_identity::NativeTlsRetention>,
    output_permit: Option<OwnedPermit>,
    deadline: Instant,
    cancelled: &'a AtomicBool,
}
impl<'a> Process<'a> {
    #[cfg(test)]
    pub(super) fn spawn(
        command: &mut Command,
        native: OwnedPermit,
        control: OwnedPermit,
        deadline: Instant,
        cancelled: &'a AtomicBool,
    ) -> Result<Self, ProcessError> {
        Self::spawn_with_tls(command, native, control, deadline, cancelled, None)
    }

    pub(super) fn spawn_with_tls(
        command: &mut Command,
        native: OwnedPermit,
        control: OwnedPermit,
        deadline: Instant,
        cancelled: &'a AtomicBool,
        tls: Option<super::client_identity::NativeTlsRetention>,
    ) -> Result<Self, ProcessError> {
        if native.request().units().items() == 0
            || native.request().units().native_bytes() == 0
            || control.request().units().control_bytes() < CONTROL_BYTES
        {
            return Err(ProcessError::InsufficientReservation);
        }
        check(deadline, cancelled)?;
        let mut child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|_| ProcessError::Spawn)?;
        let stdout = Capture::new(child.stdout.take().expect("piped child stdout"));
        let stderr = Capture::new(child.stderr.take().expect("piped child stderr"));
        let mut process = Self {
            child,
            stdout,
            stderr,
            status: None,
            failure: None,
            native: Some(native),
            control: Some(control),
            tls,
            output_permit: None,
            deadline,
            cancelled,
        };
        let configured = nonblocking(process.child.stdin.as_ref().expect("piped child stdin"))
            .and_then(|()| nonblocking(&process.stdout.pipe))
            .and_then(|()| nonblocking(&process.stderr.pipe));
        if configured.is_err() {
            process.stop()?;
            return Err(ProcessError::Io);
        }
        Ok(process)
    }

    /// Select a finite data response before sending the request. A separate
    /// transferred grant covers capture and downstream decoding; diagnostic
    /// stderr remains small. The caller reserves decoder overlap in this grant.
    pub(super) fn reserve_data_output(
        &mut self,
        limit: usize,
        permit: OwnedPermit,
    ) -> Result<(), ProcessError> {
        if limit == 0
            || permit.request().units().data_bytes() < limit as u64
            || permit.request().units().items() == 0
            || self.output_permit.is_some()
            || self.stdout.length != 0
            || self.failure.is_some()
        {
            return Err(ProcessError::InsufficientReservation);
        }
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(limit)
            .map_err(|_| ProcessError::InsufficientReservation)?;
        bytes.resize(limit, 0);
        self.stdout.bytes = bytes;
        self.output_permit = Some(permit);
        Ok(())
    }
    /// Only a joined successful process with complete clean EOF can transfer the
    /// data allocation. Unknown termination keeps the grant charged in Drop.
    pub(super) fn take_data_output(&mut self) -> Result<(Vec<u8>, OwnedPermit), ProcessError> {
        self.finish()?;
        if self.output_permit.is_none() {
            return Err(ProcessError::InsufficientReservation);
        }
        let mut bytes = std::mem::take(&mut self.stdout.bytes);
        bytes.truncate(self.stdout.length);
        self.stdout.length = 0;
        Ok((
            bytes,
            self.output_permit.take().expect("checked output grant"),
        ))
    }

    /// Close stdin, drain both bounded outputs, and positively reap the child.
    /// A complete-looking stdout buffer alone never establishes success.
    pub(super) fn finish(&mut self) -> Result<&[u8], ProcessError> {
        self.child.stdin.take();
        while self.failure.is_none() {
            let progress = match self.pump() {
                Ok(progress) => progress,
                Err(error) => {
                    self.failure = Some(error);
                    break;
                }
            };
            if let Some(status) = self.status
                && self.stdout.eof
                && self.stderr.eof
            {
                if status.success() && self.stderr.length == 0 {
                    return Ok(&self.stdout.bytes[..self.stdout.length]);
                }
                self.failure = Some(ProcessError::Exit);
                break;
            }
            if !progress {
                thread::sleep(POLL);
            }
        }
        self.stop()?;
        Err(self.failure.unwrap_or(ProcessError::Exit))
    }
    pub(super) fn failure(&self) -> Option<ProcessError> {
        self.failure
    }
    pub(super) fn terminated(&self) -> bool {
        self.status.is_some()
    }

    fn pump(&mut self) -> Result<bool, ProcessError> {
        check(self.deadline, self.cancelled)?;
        // One bounded read per pipe keeps a flood from starving cancellation,
        // the other pipe, or the child status check.
        let progress = self.stdout.read_once()? | self.stderr.read_once()?;
        self.observe_exit()?;
        Ok(progress)
    }
    fn observe_exit(&mut self) -> Result<(), ProcessError> {
        if self.status.is_none() {
            self.status = self.child.try_wait().map_err(|_| ProcessError::Io)?;
        }
        Ok(())
    }
    fn stop(&mut self) -> Result<(), ProcessError> {
        self.child.stdin.take();
        if self.terminated() {
            return Ok(());
        }
        // Child owns its unreaped PID. Never signal a numeric PID saved after
        // wait/reap, or infer cessation merely from successful kill delivery.
        let _ = self.child.kill();
        let deadline = Instant::now() + KILL_WAIT;
        loop {
            if self.observe_exit().is_ok() && self.terminated() {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(ProcessError::TerminationUnknown);
            }
            thread::sleep(POLL);
        }
    }
    fn send(&mut self, bytes: &[u8]) -> Result<usize, ProcessError> {
        if let Some(error) = self.failure {
            return Err(error);
        }
        if bytes.is_empty() {
            return Ok(0);
        }
        loop {
            self.pump()?;
            if self.terminated() {
                return Err(ProcessError::Exit);
            }
            let pipe = self.child.stdin.as_mut().ok_or(ProcessError::Io)?;
            match pipe.write(bytes) {
                Ok(0) => return Err(ProcessError::Io),
                Ok(count) => return Ok(count),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => thread::sleep(POLL),
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(_) => return Err(ProcessError::Io),
            }
        }
    }
}
impl Write for Process<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.send(bytes).map_err(|error| {
            self.failure.get_or_insert(error);
            io::Error::other("private worker input failed")
        })
    }
    fn flush(&mut self) -> io::Result<()> {
        // There is no userspace input buffer to flush.
        check(self.deadline, self.cancelled).map_err(|error| {
            self.failure.get_or_insert(error);
            io::Error::other("private worker input failed")
        })
    }
}
impl Drop for Process<'_> {
    fn drop(&mut self) {
        if self.stop().is_err() {
            // An uninterruptible kernel task is not proven dead by a deadline.
            // Fail closed: permanently charge this process's capacity, rather
            // than admitting replacement workers over possibly live native work.
            // Caller recovery must preserve the job root and exclude the old
            // process/backend; ordinary retry cannot repair this state.
            std::mem::forget(self.native.take());
            std::mem::forget(self.control.take());
            std::mem::forget(self.tls.take());
            std::mem::forget(self.output_permit.take());
        }
    }
}
fn check(deadline: Instant, cancelled: &AtomicBool) -> Result<(), ProcessError> {
    if cancelled.load(Ordering::Relaxed) {
        Err(ProcessError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(ProcessError::Deadline)
    } else {
        Ok(())
    }
}
fn nonblocking(pipe: &impl AsFd) -> rustix::io::Result<()> {
    fcntl_setfl(pipe, fcntl_getfl(pipe)? | OFlags::NONBLOCK)
}
struct Capture<T> {
    pipe: T,
    bytes: Vec<u8>,
    length: usize,
    eof: bool,
}
impl<T: Read> Capture<T> {
    fn new(pipe: T) -> Self {
        Self {
            pipe,
            bytes: vec![0; PIPE_BYTES],
            length: 0,
            eof: false,
        }
    }
    fn read_once(&mut self) -> Result<bool, ProcessError> {
        if self.eof {
            return Ok(false);
        }
        let mut excess = [0];
        let buffer = if self.length == self.bytes.len() {
            &mut excess[..]
        } else {
            let end = (self.length + PIPE_BYTES).min(self.bytes.len());
            &mut self.bytes[self.length..end]
        };
        match self.pipe.read(buffer) {
            Ok(0) => {
                self.eof = true;
                Ok(true)
            }
            Ok(_) if self.length == self.bytes.len() => Err(ProcessError::OutputLimit),
            Ok(count) => {
                self.length += count;
                Ok(true)
            }
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::Interrupted
                ) =>
            {
                Ok(false)
            }
            Err(_) => Err(ProcessError::Io),
        }
    }
}

#[cfg(test)]
mod tests;
