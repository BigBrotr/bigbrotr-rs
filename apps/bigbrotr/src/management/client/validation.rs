//! Closed response validation before a CLI publishes credentials or private JSON.
use super::ClientError;
use serde::Deserialize;

fn decimal<T: std::str::FromStr + std::fmt::Display>(value: &str) -> Result<T, ClientError> {
    value
        .parse::<T>()
        .ok()
        .filter(|n| n.to_string() == value)
        .ok_or(ClientError::Response)
}
fn positive(value: &str) -> Result<(), ClientError> {
    if decimal::<u64>(value)? == 0 {
        return Err(ClientError::Response);
    }
    Ok(())
}
pub fn session(bytes: &[u8]) -> Result<(), ClientError> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Observation<'a> {
        format: &'a str,
        principal_id: &'a str,
        credential_revision: &'a str,
        grant_revision: &'a str,
        config_revision: &'a str,
        policy_revision: &'a str,
        expires_at_us: &'a str,
        idle_expires_at_us: &'a str,
        stored_actions: Vec<&'a str>,
        authority: &'a str,
    }
    let value: Observation<'_> =
        serde_json::from_slice(bytes).map_err(|_| ClientError::Response)?;
    if value.format != "bigbrotr-session/1"
        || value.authority != "observation-only"
        || value
            .principal_id
            .parse::<bigbrotr_model::PrincipalId>()
            .is_err()
        || value.stored_actions.len() > 7
    {
        return Err(ClientError::Response);
    }
    for revision in [
        value.credential_revision,
        value.grant_revision,
        value.config_revision,
        value.policy_revision,
    ] {
        positive(revision)?;
    }
    let expires = decimal::<i64>(value.expires_at_us)?;
    let idle = decimal::<i64>(value.idle_expires_at_us)?;
    if expires <= 0 || idle <= 0 || idle > expires {
        return Err(ClientError::Response);
    }
    for (at, action) in value.stored_actions.iter().enumerate() {
        if !matches!(
            *action,
            "audit.read"
                | "budgets.change"
                | "data.read"
                | "policy.change"
                | "principals.manage"
                | "roles.control"
                | "status.read"
        ) || value.stored_actions[..at].contains(action)
        {
            return Err(ClientError::Response);
        }
    }
    Ok(())
}

// Derived Option alone also accepts omission, contrary to this wire contract.
fn nullable<'de, D, T>(d: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(d)
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Job<'a> {
    format: &'a str,
    job_id: &'a str,
    kind: &'a str,
    state: &'a str,
    job_revision: &'a str,
    checkpoint_revision: &'a str,
    created_at_us: &'a str,
    #[serde(borrow, deserialize_with = "nullable")]
    completed_at_us: Option<&'a str>,
    observed_at_us: &'a str,
    #[serde(borrow, deserialize_with = "nullable")]
    reason_code: Option<&'a str>,
    authority: &'a str,
}
pub fn page(
    bytes: &[u8],
    query: crate::management::job_list::JobListQuery,
) -> Result<(), ClientError> {
    use crate::export::execution::ExportState;
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Page<'a> {
        format: &'a str,
        #[serde(borrow)]
        items: Vec<Job<'a>>,
        effective_page_size: u16,
        #[serde(borrow, deserialize_with = "nullable")]
        next_cursor: Option<&'a str>,
        observed_at_us: &'a str,
        authority: &'a str,
    }
    let page: Page<'_> = serde_json::from_slice(bytes).map_err(|_| ClientError::Response)?;
    if page.format != "bigbrotr-job-list/1"
        || page.authority != "observation-only"
        || page.effective_page_size != query.page_size()
        || page.items.len() > usize::from(query.page_size())
        || page.next_cursor.is_some_and(|v| {
            v.is_empty()
                || v.len() > 4096
                || !v
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
        })
    {
        return Err(ClientError::Response);
    }
    decimal::<i64>(page.observed_at_us)?;
    let mut previous = None;
    let mut ids = std::collections::BTreeSet::new();
    for row in page.items {
        let state = ExportState::from_code(row.state).ok_or(ClientError::Response)?;
        let id = row
            .job_id
            .parse::<bigbrotr_model::JobId>()
            .map_err(|_| ClientError::Response)?;
        let created = decimal::<i64>(row.created_at_us)?;
        positive(row.job_revision)?;
        decimal::<u64>(row.checkpoint_revision)?;
        if let Some(time) = row.completed_at_us {
            decimal::<i64>(time)?;
        }
        let terminal = matches!(
            row.state,
            "succeeded" | "partial" | "failed" | "cancelled" | "skipped"
        );
        let needs_reason = matches!(
            row.state,
            "paused" | "cancel_requested" | "partial" | "failed" | "cancelled" | "skipped"
        );
        if row.format != "bigbrotr-job-status/1"
            || row.kind != "export"
            || row.authority != "observation-only"
            || row.observed_at_us != page.observed_at_us
            || terminal != row.completed_at_us.is_some()
            || (needs_reason && row.reason_code.is_none())
            || row
                .reason_code
                .is_some_and(|r| !matches!(r, "resource_limit" | "execution_refused" | "other"))
            || query.state().is_some_and(|expected| state != expected)
            || query.created_from_us().is_some_and(|from| created < from)
            || query
                .created_before_us()
                .is_some_and(|before| created >= before)
            || !ids.insert(id)
            || previous.is_some_and(|key| (created, id) >= key)
        {
            return Err(ClientError::Response);
        }
        previous = Some((created, id));
    }
    Ok(())
}
