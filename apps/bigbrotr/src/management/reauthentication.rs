//! One owned password check bound to an exact session and desired command.
use super::sessions::{LoginChallenge, SessionError, VerifiedLogin};
use crate::runtime::OwnedPermit;
use bigbrotr_model::{CommandKey, Digest256, ReauthenticationId};

pub(crate) struct Binding {
    pub session_id: [u8; 16],
    pub token_digest: [u8; 32],
    pub csrf_digest: [u8; 32],
    pub key: CommandKey,
    pub digest: Digest256,
    pub grant_revision: u64,
    pub config_revision: u64,
    pub policy_revision: u64,
}
pub struct ReauthenticationChallenge {
    pub(crate) login: LoginChallenge,
    pub(crate) binding: Binding,
}
pub struct VerifiedReauthentication {
    pub(crate) login: VerifiedLogin,
    pub(crate) binding: Binding,
}
impl ReauthenticationChallenge {
    /// The same owned hash reservation and supervisor join rules as login apply.
    pub fn verify(
        self,
        password: &[u8],
        permit: &mut OwnedPermit,
    ) -> Result<VerifiedReauthentication, SessionError> {
        Ok(VerifiedReauthentication {
            login: self.login.verify(password, permit)?,
            binding: self.binding,
        })
    }
}

/// Requires the original session/CSRF and the exact command at consumption.
/// No field in this response can itself authorize data access or a mutation.
pub struct ReauthenticationProof {
    pub id: ReauthenticationId,
    pub command_key: CommandKey,
    pub command_digest: Digest256,
    pub expires_at_us: i64,
}
