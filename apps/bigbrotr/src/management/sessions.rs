//! Opaque browser credentials and the bounded password work preceding issuance.
//! A verified password is deliberately not a session or an action capability.
use super::credentials::{CredentialError, random, verify_password};
use crate::runtime::OwnedPermit;
use bigbrotr_model::{DatasetId, PrincipalId, ProcessIncarnation};
use sha2::{Digest, Sha256};
use std::{fmt, time::Duration};
use zeroize::Zeroizing;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionError {
    Denied,
    Forbidden,
    Conflict,
    InvalidInput,
    Throttled,
    Capacity,
    Backend,
    Prerequisite,
    InvalidMetadata,
    Unavailable,
    /// A completed refusal due to missing native evidence, with Control healthy.
    ArchiveUnavailable,
    CommitUnknown,
    PolicyChanged,
}
impl fmt::Display for SessionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "private session refused: {self:?}")
    }
}
impl std::error::Error for SessionError {}

/// A secret has no Debug/Display/Serialize representation exposing its value.
/// Session and CSRF hashes use separate domains even for identical input bytes.
pub struct BrowserSecret(Zeroizing<[u8; 32]>);
impl fmt::Debug for BrowserSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("BrowserSecret([redacted])")
    }
}
impl BrowserSecret {
    pub(crate) fn generate() -> Result<Self, SessionError> {
        Ok(Self(Zeroizing::new(random().map_err(credential_error)?)))
    }
    pub fn parse_hex(bytes: &[u8]) -> Result<Self, SessionError> {
        if bytes.len() != 64 {
            return Err(SessionError::Denied);
        }
        let mut value = Zeroizing::new([0; 32]);
        for (target, pair) in value.iter_mut().zip(bytes.as_chunks::<2>().0) {
            let digit = |b| match b {
                b'0'..=b'9' => Ok(b - b'0'),
                b'a'..=b'f' => Ok(b - b'a' + 10),
                _ => Err(SessionError::Denied),
            };
            *target = digit(pair[0])? * 16 + digit(pair[1])?;
        }
        Ok(Self(value))
    }
    /// For the one issuance response or a request header, never routine output.
    pub fn expose_hex(&self) -> Zeroizing<String> {
        Zeroizing::new(self.0.iter().map(|b| format!("{b:02x}")).collect())
    }
    pub(crate) fn session_digest(&self) -> [u8; 32] {
        self.digest(b"bigbrotr/browser-session/1\0")
    }
    pub(crate) fn csrf_digest(&self) -> [u8; 32] {
        self.digest(b"bigbrotr/browser-csrf/1\0")
    }
    fn digest(&self, domain: &[u8]) -> [u8; 32] {
        Sha256::new()
            .chain_update(domain)
            .chain_update(&self.0[..])
            .finalize()
            .into()
    }
}

#[derive(Clone, Copy, Debug)]
pub struct SessionLifetime {
    pub(crate) maximum_us: i64,
    pub(crate) idle_us: i64,
}
impl Default for SessionLifetime {
    fn default() -> Self {
        Self {
            maximum_us: 28_800_000_000,
            idle_us: 1_800_000_000,
        }
    }
}
impl SessionLifetime {
    pub fn new(maximum: Duration, idle: Duration) -> Result<Self, SessionError> {
        fn micros(value: Duration) -> Result<i64, SessionError> {
            i64::try_from(value.as_micros())
                .ok()
                .filter(|n| *n > 0)
                .filter(|_| value.subsec_nanos().is_multiple_of(1000))
                .ok_or(SessionError::InvalidInput)
        }
        if idle > maximum {
            return Err(SessionError::InvalidInput);
        }
        Ok(Self {
            maximum_us: micros(maximum)?,
            idle_us: micros(idle)?,
        })
    }
    pub fn maximum(self) -> Duration {
        Duration::from_micros(self.maximum_us as u64)
    }
    pub fn idle(self) -> Duration {
        Duration::from_micros(self.idle_us as u64)
    }
}

pub(crate) struct LoginAccount {
    pub principal: PrincipalId,
    pub credential_revision: u64,
    pub authentication_revision: u64,
    pub salt: [u8; 16],
    pub password_hash: Zeroizing<[u8; 32]>,
}

/// Obtained only after committing a persistent account/peer attempt charge.
/// No SQL transaction is held during the expensive verification.
pub struct LoginChallenge {
    pub(crate) dataset: DatasetId,
    pub(crate) incarnation: ProcessIncarnation,
    pub(crate) issued_at_us: i64,
    pub(crate) account: Option<LoginAccount>,
}
pub struct VerifiedLogin(pub(crate) LoginChallenge);
impl LoginChallenge {
    /// The supervisor owns and joins this bounded blocking work. Cancellation
    /// must not release its permit before Argon2 and memory wiping complete.
    pub fn verify(
        self,
        password: &[u8],
        permit: &mut OwnedPermit,
    ) -> Result<VerifiedLogin, SessionError> {
        let (salt, hash) = self
            .account
            .as_ref()
            .map(|a| (&a.salt, &*a.password_hash))
            .unwrap_or((&[0; 16], &[0; 32]));
        let matched = verify_password(password, salt, hash, permit).map_err(credential_error)?;
        if !matched || self.account.is_none() {
            return Err(SessionError::Denied);
        }
        Ok(VerifiedLogin(self))
    }
}

/// Observation for /me and issuance, never reusable authorization for a command.
pub struct SessionObservation {
    pub principal: PrincipalId,
    pub credential_revision: u64,
    pub grant_revision: u64,
    pub config_revision: u64,
    pub policy_revision: u64,
    pub expires_at_us: i64,
    pub idle_expires_at_us: i64,
    pub stored_actions: Vec<String>,
}
pub struct IssuedSession {
    pub session: BrowserSecret,
    pub csrf: BrowserSecret,
    pub observation: SessionObservation,
}

fn credential_error(error: CredentialError) -> SessionError {
    match error {
        CredentialError::InvalidPassword => SessionError::Denied,
        CredentialError::Capacity => SessionError::Capacity,
        _ => SessionError::Unavailable,
    }
}
