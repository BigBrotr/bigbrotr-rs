//! One finite private CLI document, after network and credential owners terminate.
use std::{io::Write, sync::mpsc, thread, time::Duration};
use zeroize::Zeroizing;

pub(super) fn finish(bytes: Zeroizing<Vec<u8>>, code: u8, error: bool) -> ! {
    let result = emit(bytes, error);
    // A blocked writer owns only its bounded buffer and duplicate descriptor.
    // Returning would let Rust's stdio cleanup wait indefinitely. No data owner,
    // credential publisher or network driver remains at this process boundary.
    std::process::exit(i32::from(if result { code } else { 1 }));
}
fn emit(bytes: Zeroizing<Vec<u8>>, error: bool) -> bool {
    if bytes.len() > bigbrotr::management::client::RESPONSE_LIMIT + 1 {
        return false;
    }
    let fd = if error {
        rustix::io::fcntl_dupfd_cloexec(std::io::stderr(), 0)
    } else {
        rustix::io::fcntl_dupfd_cloexec(std::io::stdout(), 0)
    };
    let Ok(fd) = fd else {
        return false;
    };
    let (done, receive) = mpsc::sync_channel(1);
    if thread::Builder::new()
        .name("command-output".into())
        .stack_size(128 * 1024)
        .spawn(move || {
            let result = std::fs::File::from(fd).write_all(&bytes).is_ok();
            let _ = done.send(result);
        })
        .is_err()
    {
        return false;
    }
    receive
        .recv_timeout(Duration::from_millis(200))
        .unwrap_or(false)
}
