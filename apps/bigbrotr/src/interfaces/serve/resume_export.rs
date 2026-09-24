//! Resume keeps the committed input unit; it does not allocate a new reader pin.
use super::{Result, management, native_grants};
use bigbrotr::{
    management::sessions::SessionError,
    runtime::{ResourceBudget, ResourceRequest},
};

pub(super) async fn execute(
    work: management::ResumeWork,
    budget: &ResourceBudget,
    authority: &mut native_grants::Supervisor<'_>,
) -> Result<management::Response> {
    // Marker bytes and path/IO headroom stay charged on the actual worker even
    // if bounded shutdown cannot join it and drops the HTTP work first.
    let permit = match budget.try_acquire(ResourceRequest::data(64 * 1024, 1, 0).unwrap()) {
        Ok(permit) => permit,
        Err(_) => return Ok(work.refuse(SessionError::Capacity)),
    };
    let destination = work.destination();
    let checked = match authority
        .blocking_while(
            work.deadline(),
            || !work.abandoned(),
            move |_| {
                let result = destination
                    .check_for_acceptance()
                    .map_err(native_grants::unavailable);
                drop(permit);
                result
            },
        )
        .await?
    {
        Ok(checked) => checked,
        Err(error) => return Ok(work.refuse(error)),
    };
    let maximum = authority.maximum()?;
    // serve::run retains the original Arc through the actual Control driver
    // join even if this checked observation is dropped after unknown COMMIT.
    tokio::select! {
        biased;
        _ = authority.stop.requested() => Err((4, "export resume cancelled; Control outcome unresolved".into())),
        result = work.accept(authority.owner, maximum, &checked) => result,
    }
}
