use super::*;
use crate::runtime::{
    ProcessEnvelope, ResourceBudget, ResourceLimits, ResourceRequest, WaiterLimits,
};
use std::{io::Write, sync::atomic::Ordering, time::Duration};

fn budget() -> ResourceBudget {
    ResourceBudget::new(
        ProcessEnvelope::new(
            2 * 1024 * 1024,
            0,
            ResourceLimits::new(0, 1, 1024 * 1024, 64 * 1024),
            WaiterLimits::new(0, 0),
        )
        .unwrap(),
    )
}
fn spawn<'a>(
    command: &mut Command,
    budget: &ResourceBudget,
    cancel: &'a AtomicBool,
    duration: Duration,
) -> Process<'a> {
    Process::spawn(
        command,
        budget
            .try_acquire(ResourceRequest::data(0, 1, 1024 * 1024).unwrap())
            .unwrap(),
        budget
            .try_acquire(ResourceRequest::control(64 * 1024).unwrap())
            .unwrap(),
        Instant::now() + duration,
        cancel,
    )
    .unwrap()
}

#[test]
fn child_receives_exact_input_and_resources_stay_owned_until_reaped() {
    let budget = budget();
    let cancel = AtomicBool::new(false);
    let mut command = Command::new("/usr/bin/wc");
    command.arg("-c");
    let mut process = spawn(&mut command, &budget, &cancel, Duration::from_secs(5));
    assert_eq!(budget.snapshot().active_permits(), 2);
    let bytes = vec![b'x'; 1024 * 1024];
    process.write_all(&bytes).unwrap();
    assert_eq!(
        std::str::from_utf8(process.finish().unwrap())
            .unwrap()
            .trim(),
        "1048576"
    );
    assert!(process.terminated());
    drop(process);
    assert_eq!(budget.snapshot().active_permits(), 0);
}

#[test]
fn full_input_pipe_times_out_and_child_is_reaped_before_capacity_returns() {
    let budget = budget();
    let cancel = AtomicBool::new(false);
    let mut command = Command::new("/bin/sleep");
    command.arg("30");
    let start = Instant::now();
    let mut process = spawn(&mut command, &budget, &cancel, Duration::from_millis(150));
    assert!(process.write_all(&vec![b'x'; 4 * 1024 * 1024]).is_err());
    assert_eq!(process.failure(), Some(ProcessError::Deadline));
    assert_eq!(process.finish(), Err(ProcessError::Deadline));
    assert!(process.terminated());
    drop(process);
    assert!(start.elapsed() < Duration::from_secs(2));
    assert_eq!(budget.snapshot().active_permits(), 0);
}

#[test]
fn output_floods_are_bounded_on_both_pipes_and_do_not_hide_termination() {
    for stderr in [false, true] {
        let budget = budget();
        let cancel = AtomicBool::new(false);
        let mut command = Command::new("/bin/sh");
        command.args([
            "-c",
            if stderr {
                "exec /usr/bin/yes diagnostic >&2"
            } else {
                "exec /usr/bin/yes output"
            },
        ]);
        let start = Instant::now();
        let mut process = spawn(&mut command, &budget, &cancel, Duration::from_secs(2));
        assert_eq!(process.finish(), Err(ProcessError::OutputLimit));
        assert!(process.terminated());
        drop(process);
        assert!(start.elapsed() < Duration::from_secs(2));
        assert_eq!(budget.snapshot().active_permits(), 0);
    }
}

#[test]
fn cancellation_and_drop_terminate_live_children() {
    for explicit in [false, true] {
        let budget = budget();
        let cancel = AtomicBool::new(false);
        let mut command = Command::new("/bin/sleep");
        command.arg("30");
        let mut process = spawn(&mut command, &budget, &cancel, Duration::from_secs(20));
        let start = Instant::now();
        if explicit {
            cancel.store(true, Ordering::Relaxed);
            assert_eq!(process.finish(), Err(ProcessError::Cancelled));
            assert!(process.terminated());
        }
        drop(process);
        assert!(start.elapsed() < Duration::from_secs(2));
        assert_eq!(budget.snapshot().active_permits(), 0);
    }
}

#[test]
fn child_crash_cannot_be_successful_completion() {
    let budget = budget();
    let cancel = AtomicBool::new(false);
    let mut command = Command::new("/bin/sh");
    command.args(["-c", "kill -KILL $$"]);
    let mut process = spawn(&mut command, &budget, &cancel, Duration::from_secs(2));
    assert_eq!(process.finish(), Err(ProcessError::Exit));
    assert!(process.terminated());
    drop(process);
    assert_eq!(budget.snapshot().active_permits(), 0);
}

#[test]
fn stopped_noncooperative_child_is_killed_after_deadline() {
    let budget = budget();
    let cancel = AtomicBool::new(false);
    let mut command = Command::new("/bin/sh");
    command.args(["-c", "kill -STOP $$"]);
    let start = Instant::now();
    let mut process = spawn(&mut command, &budget, &cancel, Duration::from_millis(150));
    assert_eq!(process.finish(), Err(ProcessError::Deadline));
    assert!(process.terminated());
    drop(process);
    assert!(start.elapsed() < Duration::from_secs(2));
    assert_eq!(budget.snapshot().active_permits(), 0);
}

#[test]
fn output_limit_is_inclusive_and_one_extra_byte_refuses() {
    for (format, accepted) in [("%04096d", true), ("%04097d", false)] {
        let budget = budget();
        let cancel = AtomicBool::new(false);
        let mut command = Command::new("/usr/bin/printf");
        command.args([format, "0"]);
        let mut process = spawn(&mut command, &budget, &cancel, Duration::from_secs(2));
        if accepted {
            assert_eq!(process.finish().unwrap(), &[b'0'; 4096]);
        } else {
            assert_eq!(process.finish(), Err(ProcessError::OutputLimit));
        }
        assert!(process.terminated());
        drop(process);
        assert_eq!(budget.snapshot().active_permits(), 0);
    }
}

#[cfg(target_os = "linux")]
#[test]
fn cancellation_terminates_a_proven_full_output_pipe_without_draining_it() {
    for stderr in [false, true] {
        let budget = budget();
        let cancel = AtomicBool::new(false);
        let mut command = Command::new("/bin/sh");
        command.args([
            "-c",
            if stderr {
                "exec /usr/bin/yes x >&2"
            } else {
                "exec /usr/bin/yes x"
            },
        ]);
        let mut process = spawn(&mut command, &budget, &cancel, Duration::from_secs(5));
        let pipe = if stderr {
            process.stderr.pipe.as_fd()
        } else {
            process.stdout.pipe.as_fd()
        };
        let capacity = rustix::pipe::fcntl_getpipe_size(pipe).unwrap() as u64;
        let until = Instant::now() + Duration::from_secs(2);
        while rustix::io::ioctl_fionread(pipe).unwrap() < capacity && Instant::now() < until {
            std::thread::sleep(Duration::from_millis(2));
        }
        assert_eq!(
            rustix::io::ioctl_fionread(pipe).unwrap(),
            capacity,
            "destination must actually be full"
        );
        assert!(process.child.try_wait().unwrap().is_none());
        let start = Instant::now();
        cancel.store(true, Ordering::Relaxed);
        assert_eq!(process.finish(), Err(ProcessError::Cancelled));
        assert_eq!(
            process.stdout.length + process.stderr.length,
            0,
            "termination must not need a drain"
        );
        assert!(process.terminated());
        drop(process);
        eprintln!(
            "full-pipe stderr={stderr} capacity_bytes={capacity} termination_us={}",
            start.elapsed().as_micros()
        );
        assert!(start.elapsed() < Duration::from_secs(2));
        assert_eq!(budget.snapshot().active_permits(), 0);
    }
}
