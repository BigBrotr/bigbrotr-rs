//! Local credential custody. Files contain credentials, never cached authority.
use super::{ClientError, LoopbackOrigin};
use crate::{
    management::sessions::BrowserSecret,
    secrets::{SecretFile, read_file},
};
use rustix::fs::{
    AtFlags, FlockOperation, Mode, OFlags, RenameFlags, flock, openat, renameat_with, statat,
    unlinkat,
};
use std::{
    ffi::OsString,
    fs::File,
    io::Write,
    os::unix::fs::{MetadataExt, PermissionsExt},
};
use zeroize::Zeroizing;

pub struct Session {
    pub origin: LoopbackOrigin,
    pub session: BrowserSecret,
    pub csrf: BrowserSecret,
}
impl Session {
    pub fn open(reference: &str) -> Result<Self, ClientError> {
        let (directory, name) = parent(reference, FlockOperation::NonBlockingLockShared)?;
        let file = File::from(
            openat(
                &directory,
                &name,
                OFlags::RDONLY
                    | OFlags::NOFOLLOW
                    | OFlags::NONBLOCK
                    | OFlags::CLOEXEC
                    | OFlags::NOCTTY,
                Mode::empty(),
            )
            .map_err(|_| ClientError::SessionFile)?,
        );
        if file
            .metadata()
            .map_err(|_| ClientError::SessionFile)?
            .nlink()
            != 1
        {
            return Err(ClientError::SessionFile);
        }
        let bytes = read_file(file, 1024).map_err(|_| ClientError::SessionFile)?;
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Stored<'a> {
            format: &'a str,
            origin: &'a str,
            session: &'a str,
            csrf: &'a str,
        }
        let value: Stored<'_> =
            serde_json::from_slice(bytes.as_bytes()).map_err(|_| ClientError::SessionFile)?;
        let origin = LoopbackOrigin::parse(value.origin).map_err(|_| ClientError::SessionFile)?;
        if value.format != "bigbrotr-cli-session/1" || origin.as_string() != value.origin {
            return Err(ClientError::SessionFile);
        }
        Ok(Self {
            origin,
            session: BrowserSecret::parse_hex(value.session.as_bytes())
                .map_err(|_| ClientError::SessionFile)?,
            csrf: BrowserSecret::parse_hex(value.csrf.as_bytes())
                .map_err(|_| ClientError::SessionFile)?,
        })
    }
    pub fn authenticate(
        &self,
        request: axum::http::request::Builder,
    ) -> axum::http::request::Builder {
        let cookie = Zeroizing::new(format!(
            "BigBrotrLocalSession={}",
            self.session.expose_hex().as_str()
        ));
        let mut cookie =
            axum::http::HeaderValue::from_str(&cookie).expect("closed secret alphabet");
        cookie.set_sensitive(true);
        let mut csrf = axum::http::HeaderValue::from_str(&self.csrf.expose_hex())
            .expect("closed secret alphabet");
        csrf.set_sensitive(true);
        request
            .header("cookie", cookie)
            .header("x-csrf-token", csrf)
    }
}

fn parent(reference: &str, lock: FlockOperation) -> Result<(File, OsString), ClientError> {
    let reference = SecretFile::parse(reference).map_err(|_| ClientError::Input)?;
    let (directory, name) = reference
        .open_parent()
        .map_err(|_| ClientError::SessionFile)?;
    let metadata = directory.metadata().map_err(|_| ClientError::SessionFile)?;
    if metadata.uid() != rustix::process::geteuid().as_raw() || metadata.mode() & 0o7777 != 0o700 {
        return Err(ClientError::SessionFile);
    }
    flock(&directory, lock).map_err(|_| ClientError::Busy)?;
    Ok((directory, name.to_owned()))
}

/// Retains the checked parent and its lock from before HTTP through publication.
/// Unknown issuance/publication keeps staging; Drop must not guess it disposable.
pub struct SessionOutput {
    directory: File,
    name: OsString,
    temporary: String,
    file: File,
}
impl SessionOutput {
    pub fn prepare(path: &str) -> Result<Self, ClientError> {
        let (directory, name) = parent(
            &format!("file:{path}"),
            FlockOperation::NonBlockingLockExclusive,
        )?;
        match statat(&directory, &name, AtFlags::SYMLINK_NOFOLLOW) {
            Err(rustix::io::Errno::NOENT) => {}
            _ => return Err(ClientError::SessionFile),
        }
        let mut nonce = [0; 16];
        getrandom::fill(&mut nonce).map_err(|_| ClientError::SessionFile)?;
        let temporary = format!(
            ".bigbrotr-session-{:032x}.partial",
            u128::from_be_bytes(nonce)
        );
        let file = File::from(
            openat(
                &directory,
                temporary.as_str(),
                OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::RUSR | Mode::WUSR,
            )
            .map_err(|_| ClientError::SessionFile)?,
        );
        file.set_permissions(std::fs::Permissions::from_mode(0o600))
            .map_err(|_| ClientError::SessionFile)?;
        file.sync_all().map_err(|_| ClientError::SessionFile)?;
        directory.sync_all().map_err(|_| ClientError::SessionFile)?;
        Ok(Self {
            directory,
            name,
            temporary,
            file,
        })
    }
    /// Only an acknowledged refusal before issuance permits removing staging.
    pub fn discard(self) -> Result<(), ClientError> {
        unlinkat(&self.directory, self.temporary.as_str(), AtFlags::empty())
            .map_err(|_| ClientError::SessionFile)?;
        self.directory
            .sync_all()
            .map_err(|_| ClientError::SessionFile)
    }
    pub fn publish(mut self, session: &Session) -> Result<(), ClientError> {
        #[derive(serde::Serialize)]
        struct Stored<'a> {
            format: &'static str,
            origin: &'a str,
            session: &'a str,
            csrf: &'a str,
        }
        let origin = session.origin.as_string();
        let secret = session.session.expose_hex();
        let csrf = session.csrf.expose_hex();
        let mut bytes = Zeroizing::new(Vec::with_capacity(1024));
        serde_json::to_writer(
            &mut *bytes,
            &Stored {
                format: "bigbrotr-cli-session/1",
                origin: &origin,
                session: &secret,
                csrf: &csrf,
            },
        )
        .map_err(|_| ClientError::PublicationUnknown)?;
        self.file
            .write_all(&bytes)
            .and_then(|()| self.file.sync_all())
            .map_err(|_| ClientError::PublicationUnknown)?;
        renameat_with(
            &self.directory,
            self.temporary.as_str(),
            &self.directory,
            &self.name,
            RenameFlags::NOREPLACE,
        )
        .map_err(|_| ClientError::PublicationUnknown)?;
        self.directory
            .sync_all()
            .map_err(|_| ClientError::PublicationUnknown)?;
        let parent = File::from(
            openat(
                &self.directory,
                "..",
                OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::empty(),
            )
            .map_err(|_| ClientError::PublicationUnknown)?,
        );
        parent
            .sync_all()
            .map_err(|_| ClientError::PublicationUnknown)
    }
}
